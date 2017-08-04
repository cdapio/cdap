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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.Dataset;
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
import co.cask.cdap.hbase.wd.AbstractRowKeyDistributor;
import co.cask.cdap.hbase.wd.RowKeyDistributorByHashPrefix;
import co.cask.cdap.metrics.process.MetricsConsumerMetaTable;
import co.cask.cdap.metrics.store.upgrade.DataMigrationException;
import co.cask.cdap.metrics.store.upgrade.MetricsDataMigrator;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

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

  @Override
  public MetricsTable getV3MetricsTable(int resolution) {
    String v3TableName = cConf.get(Constants.Metrics.METRICS_TABLE_PREFIX,
                                   Constants.Metrics.DEFAULT_METRIC_V3_TABLE_PREFIX + ".ts." + resolution);
    return getResolutionMetricsTable(v3TableName);
  }

  @Override
  public MetricsTable getV2MetricsTable(int resolution) {
    String v2TableName = cConf.get(Constants.Metrics.METRICS_TABLE_PREFIX,
                                   Constants.Metrics.DEFAULT_METRIC_TABLE_PREFIX) + ".ts." + resolution;
    return getResolutionMetricsTable(v2TableName);
  }

  // todo: figure out roll time based on resolution from config? See DefaultMetricsTableFactory for example
  @Override
  public FactTable getOrCreateFactTable(int resolution) {
    String v3TableName = cConf.get(Constants.Metrics.METRICS_TABLE_PREFIX,
                                       Constants.Metrics.DEFAULT_METRIC_V3_TABLE_PREFIX) + ".ts." + resolution;
    String v2TableName = cConf.get(Constants.Metrics.METRICS_TABLE_PREFIX,
                                         Constants.Metrics.DEFAULT_METRIC_TABLE_PREFIX + ".ts." + resolution);
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

    MetricsTable table = getOrCreateResolutionMetricsTable(v2TableName, v3TableName, props);
    return new FactTable(table, entityTable.get(), resolution, getRollTime(resolution));
  }

  @Override
  public MetricsConsumerMetaTable createConsumerMeta() {
    String tableName = cConf.get(Constants.Metrics.KAFKA_META_TABLE);
    MetricsTable table = getOrCreateMetricsTable(tableName, DatasetProperties.EMPTY);
    return new MetricsConsumerMetaTable(table);
  }

  private MetricsTable getOrCreateMetricsTable(String tableName, DatasetProperties props) {
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

  /**
   * get the metrics table identified by tableName param if it exists, returns null if it doesn't exist.
   * @param tableName
   * @return
   */
  private MetricsTable getResolutionMetricsTable(String tableName) {
    try {
      // metrics tables are in the system namespace
      DatasetId tableId = NamespaceId.SYSTEM.dataset(tableName);
      MetricsTable table = null;
      if (dsFramework.hasInstance(tableId)) {
        table = getDataset(tableId);
        return table;
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return null;
  }

  private <T extends Dataset> T getDataset(DatasetId datasetInstanceId)
    throws DatasetManagementException, IOException {
    return dsFramework.getDataset(datasetInstanceId, Collections.<String, String>emptyMap(), null);
  }

  private MetricsTable getOrCreateResolutionMetricsTable(String v2TableName, String v3TableName,
                                                 TableProperties.Builder props) {
    try {
      // metrics tables are in the system namespace
      DatasetId v2TableId = NamespaceId.SYSTEM.dataset(v2TableName);

      MetricsTable v2Table = null;
      if (dsFramework.hasInstance(v2TableId)) {
        v2Table = DatasetsUtil.getOrCreateDataset(dsFramework, v2TableId, MetricsTable.class.getName(),
                                                  props.build(), null);
      }

      props.add(HBaseTableAdmin.PROPERTY_SPLITS,
                GSON.toJson(getV3MetricsTableSplits(Constants.Metrics.METRICS_HBASE_SPLITS)));
      props.add(Constants.Metrics.METICS_HBASE_TABLE_SPLITS, Constants.Metrics.METRICS_HBASE_SPLITS);
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
    return getSplitKeys(splits, splits, rowKeyDistributor);
  }

  public static byte[][] getSplitKeys(int splits, int buckets, AbstractRowKeyDistributor keyDistributor) {
    // "1" can be used for queue tables that we know are not "hot", so we do not pre-split in this case
    if (splits == 1) {
      return new byte[0][];
    }

    byte[][] bucketSplits = keyDistributor.getAllDistributedKeys(Bytes.EMPTY_BYTE_ARRAY);
    Preconditions.checkArgument(splits >= 1 && splits <= 0xff * bucketSplits.length,
                                "Number of pre-splits should be in [1.." +
                                  0xff * bucketSplits.length + "] range");


    // Splits have format: <salt bucket byte><extra byte>. We use extra byte to allow more splits than buckets:
    // salt bucket bytes are usually sequential in which case we cannot insert any value in between them.

    int splitsPerBucket = (splits + buckets - 1) / buckets;
    splitsPerBucket = splitsPerBucket == 0 ? 1 : splitsPerBucket;

    byte[][] splitKeys = new byte[bucketSplits.length * splitsPerBucket - 1][];

    int prefixesPerSplitInBucket = (0xff + 1) / splitsPerBucket;

    for (int i = 0; i < bucketSplits.length; i++) {
      for (int k = 0; k < splitsPerBucket; k++) {
        if (i == 0 && k == 0) {
          // hbase will figure out first split
          continue;
        }
        int splitStartPrefix = k * prefixesPerSplitInBucket;
        int thisSplit = i * splitsPerBucket + k - 1;
        if (splitsPerBucket > 1) {
          splitKeys[thisSplit] = new byte[] {(byte) i, (byte) splitStartPrefix};
        } else {
          splitKeys[thisSplit] = new byte[] {(byte) i};
        }
      }
    }

    return splitKeys;
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
