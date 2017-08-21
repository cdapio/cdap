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
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import co.cask.cdap.proto.id.NamespaceId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Useful to get v2/v3 metrics tables with retry and check if metrics table exists or not
 */
public class MigrationTableUtility {
  private static final Logger LOG = LoggerFactory.getLogger(MigrationTableUtility.class);

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final MetricDatasetFactory metricDatasetFactory;
  private final int resolution;
  private final HBaseTableUtil tableUtil;

  MigrationTableUtility(CConfiguration cConfiguration, Configuration hConf,
                        MetricDatasetFactory metricDatasetFactory, HBaseTableUtil tableUtil, int resolution) {
    this.cConf = cConfiguration;
    this.hConf = hConf;
    this.metricDatasetFactory = metricDatasetFactory;
    this.tableUtil = tableUtil;
    this.resolution = resolution;
  }

  // we try to get v2 metrics table with retry, there is a chance that v2 table got deleted before we got it
  // we return null if it was already deleted
  @Nullable
   MetricsTable getV2MetricsTable() {
    MetricsTable v2table = null;
    while (v2table == null) {
      try {
        v2table = metricDatasetFactory.getV2MetricsTable(resolution);
      } catch (Exception e) {
        if (e instanceof TableNotFoundException) {
          // we checked table exists, however the table got deleted in-between before we got the table
          return null;
        }
        LOG.warn("Cannot access v2 metricsTable, will retry in 1 sec.");
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    return v2table;
  }

  // we try to get v3 metrics table with retry
  MetricsTable getV3MetricsTable() {
    MetricsTable v3table = null;
    while (v3table == null) {
      try {
        v3table = metricDatasetFactory.getV3MetricsTable(resolution);
      } catch (Exception e) {
        LOG.warn("Cannot access v3 metricsTable, will retry in 1 sec.");
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    return v3table;
  }

  /**
   * check if v2 metrics table exists for the resolution
   * @return true if table exists; false otherwise
   */
  boolean v2MetricsTableExists() {
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
      LOG.warn("Exception while checking table exists", e);
    }
    return false;
  }



  private TableId getV2MetricsTableName(int resolution) {
    String v2TableName = cConf.get(Constants.Metrics.METRICS_TABLE_PREFIX,
                                   Constants.Metrics.DEFAULT_METRIC_TABLE_PREFIX) + ".ts." + resolution;
    return TableId.from(NamespaceId.SYSTEM.getNamespace(), v2TableName);
  }
}
