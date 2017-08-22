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

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Migrates data from v2 table to v3 table with salting.
 */
public class DataMigrator {
  private static final Logger LOG = LoggerFactory.getLogger(DataMigrator.class);

  private final LinkedHashMap<Integer, Integer> maxRecordsToScanResolutionMap;
  private final ScheduledExecutorService scheduledExecutorService;
  private final MigrationTableUtility migrationTableUtility;
  private final CConfiguration cConf;

  // Data migrator thread will be created for each resolution tables, so they can be scheduled appropriately.
  public DataMigrator(MetricDatasetFactory metricDatasetFactory, CConfiguration cConf, Configuration hConf,
                      HBaseTableUtil hBaseTableUtil, LinkedHashMap<Integer, Integer> maxRecordsToScanResolutionMap,
                      ScheduledExecutorService executorService) {
    this.migrationTableUtility = new MigrationTableUtility(cConf, hConf, metricDatasetFactory, hBaseTableUtil);
    this.maxRecordsToScanResolutionMap = maxRecordsToScanResolutionMap;
    this.scheduledExecutorService = executorService;
    this.cConf = cConf;
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
    Iterator<Integer> resolutions = maxRecordsToScanResolutionMap.keySet().iterator();

    if (!resolutions.hasNext()) {
      // all tables in migrationOrder map have been deleted, so migration is complete;
      // useful when metrics.processor is running and completed migration
      // and the run method removed all the entries from map when the tables were deleted
      return true;
    }

    // when metrics.processor restarts and initializes DataMigrator,
    // this is useful to decide whether to schedule migration thread or not
    while (resolutions.hasNext()) {
      int resolution = resolutions.next();
      if (migrationTableUtility.v2MetricsTableExists(resolution)) {
        try (MetricsTable v3MetricsTable = migrationTableUtility.getV3MetricsTable(resolution);
             MetricsTable v2MetricsTable = migrationTableUtility.getV2MetricsTable(resolution)) {
          if (v2MetricsTable == null || v3MetricsTable == null) {
            // dataset/hbase service not available, skipping check this time, will try in next schedule.
            // return false to try again in next attempt,
            // by false we will proceed to run the migration, however if all tables have been migrated
            // migrationTask can handle that scenario and shutdown the executor service.
            return false;
          }
          MetricsTableMigration metricsTableMigration =
            new MetricsTableMigration(v2MetricsTable, v3MetricsTable, cConf);
          // if metrics data is available in old table, return false
          if (metricsTableMigration.isOldMetricsDataAvailable()) {
            return false;
          }
        } catch (IOException e) {
          // exception while closing metrics tables - ignoring
        }
      }
    }
    // all tables data have been migrated, so migration is complete
    return true;
  }

  private final class MigrationTask implements Runnable {
    public void run() {

      Iterator<Integer> resolutions = maxRecordsToScanResolutionMap.keySet().iterator();
      // iterate through resolutions, remove entry from iterator
      // if the table has been deleted for that resolution or if there are no more data available in the table
      while (resolutions.hasNext()) {
        int resolution = resolutions.next();
        if (migrationTableUtility.v2MetricsTableExists(resolution)) {
          try (MetricsTable v3MetricsTable = migrationTableUtility.getV3MetricsTable(resolution);
               MetricsTable v2MetricsTable = migrationTableUtility.getV2MetricsTable(resolution)) {
            if (v2MetricsTable == null || v3MetricsTable == null) {
              // dataset/hbase service not available, return,
              // will try to get table and perform migration in next schedule.
              return;
            }

            MetricsTableMigration metricsTableMigration =
              new MetricsTableMigration(v2MetricsTable, v3MetricsTable, cConf);

            if (metricsTableMigration.isOldMetricsDataAvailable()) {
              LOG.info("Metrics data is available in v2 metrics - {} resolution table.. Running Migration", resolution);
              metricsTableMigration.transferData(maxRecordsToScanResolutionMap.get(resolution));
              break;
            } else {
              // no more metrics data is available for transfer, remove from iterator
              resolutions.remove();
            }
          } catch (IOException e) {
            // exception while closing metrics tables - ignoring
          }
        } else {
          // table does not exist, remove from map
          resolutions.remove();
        }
      }

      if (maxRecordsToScanResolutionMap.isEmpty()) {
        // this will be printed once so safe to log it in info
        LOG.info("Metrics migration complete for resolution INT_MAX, 3600, 60 table");
        scheduledExecutorService.shutdown();
        return;
      }
    }
  }

}
