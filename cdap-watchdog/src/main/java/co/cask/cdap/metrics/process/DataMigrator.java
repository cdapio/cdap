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
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Migrates data from v2 table to v3 table with salting.
 */
public class DataMigrator implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(DataMigrator.class);

  private final LinkedHashMap<Integer, MetricsTableMigration> migrationOrder;
  private final Map<Integer, Integer> maxRecordsToScanResolutionMap;
  private final HBaseTableUtil hBaseTableUtil;

  // Data migrator thread will be created for each resolution tables, so they can be scheduled appropriately.
  public DataMigrator(MetricDatasetFactory metricDatasetFactory, CConfiguration cConf, Configuration hConf,
                      HBaseTableUtil hBaseTableUtil, LinkedHashMap<Integer, Integer> maxRecordsToScanResolutionMap) {
    migrationOrder = new LinkedHashMap<>();
    for (int resolution : maxRecordsToScanResolutionMap.keySet()) {
      migrationOrder.put(resolution, new MetricsTableMigration(metricDatasetFactory, resolution, cConf, hConf));
    }
    this.maxRecordsToScanResolutionMap = maxRecordsToScanResolutionMap;
    this.hBaseTableUtil = hBaseTableUtil;
  }

  /**
   * returns true when data migration is complete and all v2 metrics tables have been deleted; false otherwise
   * @return true if migration is complete; false otherwise
   */
  public boolean isMigrationComplete() {
    if (migrationOrder.isEmpty()) {
      // all tables have been deleted, so migration is complete;
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
      if (metricsTableMigration.v2MetricsTableExists(hBaseTableUtil, resolution)) {
        return false;
      }
    }
    // all tables have been deleted, so migration is complete
    return true;
  }

  @Override
  public void run() {
    if (migrationOrder.isEmpty()) {
      LOG.debug("All data from old tables have been migrated, not running migration");
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
