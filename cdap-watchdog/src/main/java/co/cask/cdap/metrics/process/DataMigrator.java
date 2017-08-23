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

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Migrates data from v2 table to v3 table with salting.
 */
public class DataMigrator extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(DataMigrator.class);

  private final List<Integer> resolutions;
  private final DatasetFramework datasetFramework;
  private final String v2TableNamePrefix;
  private final String v3TableNamePrefix;
  private final int sleepMillisBetweenTransfer;

  /**
   * Data migrator to run data migration for metrics tables from v2 to v3 metrics tables.
   * @param resolutions - list of resolution tables to migrate data from
   * @param sleepMillisBetweenTransfer while the data transfer is running -
   *                                   amount of time to sleep between each record transfer
   */
  public DataMigrator(DatasetFramework datasetFramework, List<Integer> resolutions,
                      String v2TableNamePrefix, String v3TableNamePrefix, int sleepMillisBetweenTransfer) {
    this.datasetFramework = datasetFramework;
    this.resolutions = resolutions;
    this.v2TableNamePrefix = v2TableNamePrefix;
    this.v3TableNamePrefix = v3TableNamePrefix;
    this.sleepMillisBetweenTransfer = sleepMillisBetweenTransfer;
  }

  @Override
  public void run() {
    // iterate through resolutions, if datasetFramework has v2metricsTable then try to get V2 table with a retry
    // once you get v2 table, get v3 table and run migration
    // if v2 table does not exist continue to next resolution
    // if no v2 tables exist then exit this thread - data migration is complete.
    for (int resolution : resolutions) {
      try {
        DatasetId v2MetricsTableId = getMetricsDatasetId(v2TableNamePrefix, resolution);
        DatasetId v3MetricsDatasetId = getMetricsDatasetId(v3TableNamePrefix, resolution);
        if (MigrationTableHelper.hasInstanceWithRetry(datasetFramework, v2MetricsTableId)) {
          try (MetricsTable v2MetricsTable =
                 MigrationTableHelper.getDatasetWithRetry(datasetFramework, v2MetricsTableId);
               MetricsTable v3MetricsTable =
                 MigrationTableHelper.getDatasetWithRetry(datasetFramework, v3MetricsDatasetId)) {
            MetricsTableMigration metricsTableMigration = new MetricsTableMigration(v2MetricsTable, v3MetricsTable);
            metricsTableMigration.transferData(sleepMillisBetweenTransfer);
            LOG.info("Metrics table data migration is complete for resolution {}", resolution);
            // now transfer is complete; its safe to delete the v2 metrics table
            MigrationTableHelper.deleteInstanceWithRetry(datasetFramework, v3MetricsDatasetId);
            LOG.debug("Deleted Metrics table for resolution {}", resolution);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      } catch (DatasetManagementException | IOException e) {
        LOG.error("Exception while performing dataset operation during metrics table migration", e);
        // TODO : confirm if this is okay
        return;
      }
    }
  }

  private DatasetId getMetricsDatasetId(String tableNamePrefix, int resolution) {
    String tableName =  tableNamePrefix + resolution;
    return NamespaceId.SYSTEM.dataset(tableName);
  }
}
