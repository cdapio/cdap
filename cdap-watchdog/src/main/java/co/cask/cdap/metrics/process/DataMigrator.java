/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.metrics.process;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Migrates data from v2 table to v3 table with salting.
 */
public class DataMigrator {
  private static final Logger LOG = LoggerFactory.getLogger(DataMigrator.class);

  private final LinkedHashMap<Integer, MetricsTableMigration> migrationOrder;
  private final Map<Integer, Integer> maxRecordsToScanResolutionMap;
  private final HBaseTableUtil hBaseTableUtil;
  private final ScheduledExecutorService scheduledExecutorService;


  // Data migrator thread will be created for each resolution tables, so they can be scheduled appropriately.
  public DataMigrator(MetricDatasetFactory metricDatasetFactory, CConfiguration cConf, Configuration hConf,
                      HBaseTableUtil hBaseTableUtil, LinkedHashMap<Integer, Integer> maxRecordsToScanResolutionMap,
                      ScheduledExecutorService executorService) {
    migrationOrder = new LinkedHashMap<>();

    for (int resolution : maxRecordsToScanResolutionMap.keySet()) {
      MigrationTableUtility migrationTableUtility = new MigrationTableUtility(cConf, hConf,
                                                                              metricDatasetFactory, hBaseTableUtil,
                                                                              resolution);
      if (migrationTableUtility.v2MetricsTableExists()) {
        MetricsTable v3MetricsTable = migrationTableUtility.getV3MetricsTable();
        MetricsTable v2MetricsTable = migrationTableUtility.getV2MetricsTable();
        if (v2MetricsTable == null) {
          // table got deleted, skip this resolution
          continue;
        }
        migrationOrder.put(resolution, new MetricsTableMigration(v2MetricsTable, v3MetricsTable, cConf, hConf));
      }
    }
    this.maxRecordsToScanResolutionMap = maxRecordsToScanResolutionMap;
    this.hBaseTableUtil = hBaseTableUtil;
    this.scheduledExecutorService = executorService;
  }

  /**
   * if the metrics data migration is already completed, we would just shutdown the executor service and return
   * if not, we would schedule migration task at fixed rate schedule.
   * when the migration is complete, the task would shutdown the executor service by itself
   */
  public void scheduleMigrationIfNecessary() {
    if (!isMigrationComplete()) {
      // we have 5 minute initial delay for availability of transaction/dataset service
      // to be able to get the metrics table or if v3 tables are not already created they might take some time
      // initially, we don't have to perform
      scheduledExecutorService.scheduleAtFixedRate(new MigrationTask(), 5, 1, TimeUnit.MINUTES);
      LOG.info("Scheduled metrics migration thread for resolution INT_MAX, 3600, 60 tables");
    } else {
      scheduledExecutorService.shutdown();
    }
  }

  /**
   * returns true when data migration is complete and
   * all v2 metrics tables data has been migrated for configured resolutions ; false otherwise
   * @return true if migration is complete; false otherwise
   */
  private boolean isMigrationComplete() {
    if (migrationOrder.isEmpty()) {
      // all tables in migrationOrder map have been deleted, so migration is complete;
      // useful when metrics.processor is running and completed migration
      // and the run method removed all the entries from map when the tables were deleted
      return true;
    }

    // when metrics.processor restarts and initializes DataMigrator,
    // this is useful to decide whether to schedule migration thread or not
    Iterator<Integer> resolutions = migrationOrder.keySet().iterator();
    while (resolutions.hasNext()) {
      int resolution = resolutions.next();
      MetricsTableMigration metricsTableMigration = migrationOrder.get(resolution);
      if (metricsTableMigration.v2MetricsTableExists(hBaseTableUtil, resolution) &&
        metricsTableMigration.isOldMetricsDataAvailable()) {
        return false;
      }
    }
    // all tables data have been migrated, so migration is complete
    return true;
  }

  private final class MigrationTask implements Runnable {
    public void run() {
      if (isMigrationComplete()) {
        // this will be printed once as the task is scheduled only if migration is not complete
        // for future metrics.processor runs
        LOG.info("Metrics migration complete for resolution INT_MAX, 3600, 60 table");
        scheduledExecutorService.shutdown();
        return;
      }

      Iterator<Integer> resolutions = migrationOrder.keySet().iterator();
      while (resolutions.hasNext()) {
        int resolution = resolutions.next();
        MetricsTableMigration metricsTableMigration = migrationOrder.get(resolution);

        // check if table exists first
        if (!metricsTableMigration.v2MetricsTableExists(hBaseTableUtil, resolution)) {
          LOG.debug("Table with resolution {} does not exist, removing from list", resolution);
          resolutions.remove();
          continue;
        }

        if (metricsTableMigration.isOldMetricsDataAvailable()) {
          LOG.info("Metrics data is available in v2 metrics - {} resolution table.. Running Migration", resolution);
          metricsTableMigration.transferData(maxRecordsToScanResolutionMap.get(resolution));
          break;
        }
      }
    }
  }

}
