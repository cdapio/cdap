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
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Deletes old v2 metrics tables when they no longer have any data. ie - data migration to new v3 table is complete
 */
public class MetricsTableDeleter {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsTableDeleter.class);

  private final List<Integer> resolutionsToDelete;
  private final MigrationTableUtility migrationTableUtility;
  private final ScheduledExecutorService scheduledExecutorService;
  private final CConfiguration cConf;

  public MetricsTableDeleter(MetricDatasetFactory metricDatasetFactory, CConfiguration cConf, Configuration hConf,
                             HBaseTableUtil hBaseTableUtil, List<Integer> resolutions,
                             ScheduledExecutorService scheduledExecutorService) {
    this.resolutionsToDelete = resolutions;
    this.migrationTableUtility = new MigrationTableUtility(cConf, hConf, metricDatasetFactory, hBaseTableUtil);
    this.scheduledExecutorService = scheduledExecutorService;
    this.cConf = cConf;
  }

  /**
   * if the metrics data migration  and all tables are deleted, we would just shutdown the executor service and return
   * if not, we would schedule deletion task at fixed rate schedule.
   * when the deletion of tables is complete, the task would shutdown the executor service by itself
   */
  public void scheduleDeletionIfNecessary() {
    if (!allTablesDeleted()) {
      scheduledExecutorService.scheduleAtFixedRate(new DeletionTask(), 1, 2, TimeUnit.HOURS);
      LOG.info("Scheduled metrics deletion thread for 1, 60, 3600, INT_MAX resolution tables, " +
                 "tables will only be deleted after data migration is completed for them");
    } else {
      LOG.debug("Not Scheduling Deletion thread, all tables have already been deleted");
      scheduledExecutorService.shutdown();
    }
  }

  /**
   * returns true when data migration is complete and all v2 metrics tables have been deleted; false otherwise
   * @return true if migration is complete; false otherwise
   */
  private boolean allTablesDeleted() {
    // when metrics.processor restarts and initializes TableDeleter,
    // this is useful to decide whether to schedule deletion thread or not
    for (int resolution : resolutionsToDelete) {
      if (migrationTableUtility.v2MetricsTableExists(resolution)) {
        return false;
      }
    }
    // all tables have been deleted already, so migration is complete
    return true;
  }

  private final class DeletionTask implements Runnable {
    @Override
    public void run() {
      if (allTablesDeleted()) {
        // this log will be printed only once when all tables are deleted
        // and the future runs will be skipped earlier before scheduling the Deletion task
        LOG.info("All Metrics tables have been deleted successfully");
        scheduledExecutorService.shutdown();
        return;
      }

      for (int resolution : resolutionsToDelete) {
        if (migrationTableUtility.v2MetricsTableExists(resolution)) {
          try (MetricsTable v3MetricsTable = migrationTableUtility.getV3MetricsTable(resolution);
          MetricsTable v2MetricsTable = migrationTableUtility.getV2MetricsTable(resolution)) {
            if (v2MetricsTable == null || v3MetricsTable == null) {
              // dataset/hbase service not available, skipping check this time, will try in next scheduled run.
              return;
            }
            MetricsTableMigration metricsTableMigration =
              new MetricsTableMigration(v2MetricsTable, v3MetricsTable, cConf);
            if (!metricsTableMigration.isOldMetricsDataAvailable() &&
              migrationTableUtility.deleteV2MetricsTable(resolution)) {
              LOG.info("Successfully Deleted the v2 metrics table for resolution {} seconds", resolution);
            }
          } catch (IOException e) {
            // exception while closing metrics-tables - ignoring
          }
        }
      }
    }
  }

}
