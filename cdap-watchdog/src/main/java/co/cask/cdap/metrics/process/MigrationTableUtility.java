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
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseDDLExecutorFactory;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.spi.hbase.HBaseDDLExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * To get v2/v3 metrics tables; perform tableExists/delete table on v2 metrics tables
 */
public class MigrationTableUtility {
  private static final Logger LOG = LoggerFactory.getLogger(MigrationTableUtility.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final MetricDatasetFactory metricDatasetFactory;
  private final HBaseDDLExecutorFactory hBaseDDLExecutorFactory;
  private final HBaseTableUtil tableUtil;

  MigrationTableUtility(CConfiguration cConfiguration, Configuration hConf,
                        MetricDatasetFactory metricDatasetFactory, HBaseTableUtil tableUtil) {
    this.cConf = cConfiguration;
    this.hConf = hConf;
    this.metricDatasetFactory = metricDatasetFactory;
    this.hBaseDDLExecutorFactory = new HBaseDDLExecutorFactory(cConf, hConf);
    this.tableUtil = tableUtil;
  }

  // we try to get v2 metrics table; if there is any exception while getting the table we return null
  @Nullable
  MetricsTable getV2MetricsTable(int resolution) {
    MetricsTable v2table = null;
    try {
      v2table = metricDatasetFactory.getV2MetricsTable(resolution);
    } catch (Exception e) {
      LOG.info("Cannot access v2 metricsTable due to exception", e);
    }
    return v2table;
  }

  // we try to get v3 metrics table; if there is any exception while getting the table we return null
  @Nullable
  MetricsTable getV3MetricsTable(int resolution) {
    MetricsTable v3table = null;
    try {
      v3table = metricDatasetFactory.getV3MetricsTable(resolution);
    } catch (Exception e) {
      LOG.info("Cannot access v3 metricsTable due to exception", e);
    }
    return v3table;
  }

  /**
   * check if v2 metrics table exists for the resolution
   * @return true if table exists; false otherwise
   */
  boolean v2MetricsTableExists(int resolution) {
    TableId tableId = getV2MetricsTableName(resolution);
    try {
      try (HBaseAdmin admin = new HBaseAdmin(hConf)) {
        TableId hBaseTableId =
          tableUtil.createHTableId(new NamespaceId(tableId.getNamespace()), tableId.getTableName());
        boolean doesExist  = tableUtil.tableExists(admin, hBaseTableId);
        LOG.debug("Table {} exists : {}", hBaseTableId.getTableName(), doesExist);
        return doesExist;
      }
    } catch (IOException e) {
      LOG.info("Exception while checking table exists", e);
    }
    return false;
  }

  /**
   * delete v2 metrics table for the resolution
   * @param resolution resolution of metrics table to delete
   * @return true if deletion is successful; false otherwise
   */
  boolean deleteV2MetricsTable(int resolution) {
    TableId tableId = getV2MetricsTableName(resolution);
    try {
      try (HBaseDDLExecutor ddlExecutor = hBaseDDLExecutorFactory.get(); HBaseAdmin admin = new HBaseAdmin(hConf)) {
        TableId hBaseTableId =
          tableUtil.createHTableId(new NamespaceId(tableId.getNamespace()), tableId.getTableName());
        if (tableUtil.tableExists(admin, hBaseTableId)) {
          LOG.trace("Found table {}, going to delete", hBaseTableId);
          tableUtil.dropTable(ddlExecutor, hBaseTableId);
          LOG.debug("Deleted table {}", hBaseTableId);
          return true;
        }
      }
    } catch (IOException e) {
      LOG.warn("Exception while deleting table", e);
    }
    return false;
  }

  private TableId getV2MetricsTableName(int resolution) {
    String v2TableName = cConf.get(Constants.Metrics.METRICS_TABLE_PREFIX,
                                   Constants.Metrics.DEFAULT_METRIC_TABLE_PREFIX) + ".ts." + resolution;
    return TableId.from(NamespaceId.SYSTEM.getNamespace(), v2TableName);
  }
}
