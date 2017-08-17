/*
 * Copyright 2015-2017 Cask Data, Inc.
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

package co.cask.cdap.metrics.store;

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.TableProperties;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.hbase.CombinedHBaseMetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseTableAdmin;
import co.cask.cdap.data2.dataset2.lib.timeseries.EntityTable;
import co.cask.cdap.data2.dataset2.lib.timeseries.FactTable;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.hbase.wd.RowKeyDistributorByHashPrefix;
import co.cask.cdap.metrics.process.MetricsConsumerMetaTable;
import co.cask.cdap.metrics.store.upgrade.DataMigrationException;
import co.cask.cdap.metrics.store.upgrade.MetricsDataMigrator;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 */
public class DefaultMetricDatasetFactory implements MetricDatasetFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetricDatasetFactory.class);
  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;
  private final Supplier<EntityTable> entityTable;
  private final DatasetFramework dsFramework;

  @Inject
  public DefaultMetricDatasetFactory(final CConfiguration cConf, DatasetFramework dsFramework) {
    this.cConf = cConf;
    this.dsFramework = dsFramework;
    this.entityTable = Suppliers.memoize(new Supplier<EntityTable>() {
      @Override
      public EntityTable get() {
        String tableName = cConf.get(Constants.Metrics.ENTITY_TABLE_NAME,
                                     Constants.Metrics.DEFAULT_ENTITY_TABLE_NAME);
        return new EntityTable(getOrCreateMetricsTable(tableName, DatasetProperties.EMPTY));
      }
    });
  }

  // todo: figure out roll time based on resolution from config? See DefaultMetricsTableFactory for example
  @Override
  public FactTable getOrCreateFactTable(int resolution) {
    String v3TableName = cConf.get(Constants.Metrics.METRICS_TABLE_PREFIX,
                                       Constants.Metrics.DEFAULT_METRIC_V3_TABLE_PREFIX) + ".ts." + resolution;

    int ttl = cConf.getInt(Constants.Metrics.RETENTION_SECONDS + "." + resolution + ".seconds", -1);

    TableProperties.Builder props = TableProperties.builder();
    // don't add TTL for MAX_RESOLUTION table. CDAP-1626
    if (ttl > 0 && resolution != Integer.MAX_VALUE) {
      props.setTTL(ttl);
    }
    // for efficient counters
    props.setReadlessIncrementSupport(true);
    // configuring pre-splits
    props.add(HBaseTableAdmin.PROPERTY_SPLITS,
              GSON.toJson(FactTable.getSplits(DefaultMetricStore.AGGREGATIONS.size())));

    MetricsTable table = getOrCreateResolutionMetricsTable(v3TableName, props, resolution);
    return new FactTable(table, entityTable.get(), resolution, getRollTime(resolution));
  }

  @Override
  public MetricsConsumerMetaTable createConsumerMeta() {
    String tableName = cConf.get(Constants.Metrics.KAFKA_META_TABLE);
    MetricsTable table = getOrCreateMetricsTable(tableName, DatasetProperties.EMPTY);
    return new MetricsConsumerMetaTable(table);
  }

  protected MetricsTable getOrCreateMetricsTable(String tableName, DatasetProperties props) {
    // metrics tables are in the system namespace
    DatasetId metricsDatasetInstanceId = NamespaceId.SYSTEM.dataset(tableName);
    MetricsTable table = null;
    try {
      table = DatasetsUtil.getOrCreateDataset(dsFramework, metricsDatasetInstanceId, MetricsTable.class.getName(),
                                              props, null);
    } catch (Exception e) {
      Throwables.propagate(e);
    }
    return table;
  }

  protected MetricsTable getOrCreateResolutionMetricsTable(String v3TableName, TableProperties.Builder props,
                                                           int resolution) {
    try {
      MetricsTable v2Table = null;

      // TODO: By default do not allow reading from v2 tables. Remove this check once CDAP-12306 is fixed
      if (cConf.getBoolean(Constants.Metrics.METRICS_V2_TABLE_SCAN_ENABLED)) {
        String v2TableName = cConf.get(Constants.Metrics.METRICS_TABLE_PREFIX,
                                       Constants.Metrics.DEFAULT_METRIC_TABLE_PREFIX + ".ts." + resolution);
        // metrics tables are in the system namespace
        DatasetId v2TableId = NamespaceId.SYSTEM.dataset(v2TableName);
        v2Table = dsFramework.getDataset(v2TableId, ImmutableMap.<String, String>of(), null);
      }

      props.add(HBaseTableAdmin.PROPERTY_SPLITS,
                GSON.toJson(getV3MetricsTableSplits(Constants.Metrics.METRICS_HBASE_SPLITS)));
      props.add(Constants.Metrics.METRICS_HBASE_MAX_SCAN_THREADS,
                cConf.get(Constants.Metrics.METRICS_HBASE_MAX_SCAN_THREADS));

      DatasetId v3TableId = NamespaceId.SYSTEM.dataset(v3TableName);
      MetricsTable v3Table = DatasetsUtil.getOrCreateDataset(dsFramework, v3TableId, MetricsTable.class.getName(),
                                                             props.build(), null);

      if (v2Table != null) {
        // the cluster is upgraded, so use Combined Metrics Table
        return new CombinedHBaseMetricsTable(v2Table, v3Table);
      }

      return v3Table;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private static byte[][] getV3MetricsTableSplits(int splits) {
    RowKeyDistributorByHashPrefix rowKeyDistributor = new RowKeyDistributorByHashPrefix(
      new RowKeyDistributorByHashPrefix.OneByteSimpleHash(splits));
    return rowKeyDistributor.getSplitKeys(splits, splits);
  }


  /**
   * Creates the metrics tables and kafka-meta table using the factory {@link DefaultMetricDatasetFactory}
   * <p>
   * It is primarily used by upgrade and data-migration tool.
   *
   * @param factory : metrics dataset factory
   */
  public static void setupDatasets(DefaultMetricDatasetFactory factory)
    throws IOException, DatasetManagementException {
    // adding all fact tables
    factory.getOrCreateFactTable(1);
    factory.getOrCreateFactTable(60);
    factory.getOrCreateFactTable(3600);
    factory.getOrCreateFactTable(Integer.MAX_VALUE);

    // adding kafka consumer meta
    factory.createConsumerMeta();
  }

  /**
   * Migrates metrics data from version 2.7 and older to 2.8
   *
   * @param conf             CConfiguration
   * @param hConf            Configuration
   * @param datasetFramework framework to add types and datasets to
   * @param keepOldData      - boolean flag to specify if we have to keep old metrics data
   * @throws DataMigrationException
   */
  public static void migrateData(CConfiguration conf, Configuration hConf, DatasetFramework datasetFramework,
                                 boolean keepOldData, HBaseTableUtil tableUtil) throws DataMigrationException {
    DefaultMetricDatasetFactory factory = new DefaultMetricDatasetFactory(conf, datasetFramework);
    MetricsDataMigrator migrator = new MetricsDataMigrator(conf, hConf, datasetFramework, factory);
    // delete existing destination tables
    migrator.cleanupDestinationTables();
    try {
      setupDatasets(factory);
    } catch (Exception e) {
      String msg = "Exception creating destination tables";
      LOG.error(msg, e);
      throw new DataMigrationException(msg);
    }
    migrator.migrateMetricsTables(tableUtil, keepOldData);
  }

  private int getRollTime(int resolution) {
    String key = Constants.Metrics.TIME_SERIES_TABLE_ROLL_TIME + "." + resolution;
    String value = cConf.get(key);
    if (value != null) {
      return cConf.getInt(key, Constants.Metrics.DEFAULT_TIME_SERIES_TABLE_ROLL_TIME);
    }
    return cConf.getInt(Constants.Metrics.TIME_SERIES_TABLE_ROLL_TIME,
                        Constants.Metrics.DEFAULT_TIME_SERIES_TABLE_ROLL_TIME);
  }
}
