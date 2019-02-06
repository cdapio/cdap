/*
 * Copyright 2015-2019 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetContext;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.TableProperties;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseTableAdmin;
import co.cask.cdap.data2.dataset2.lib.timeseries.EntityTable;
import co.cask.cdap.data2.dataset2.lib.timeseries.FactTable;
import co.cask.cdap.hbase.wd.RowKeyDistributorByHashPrefix;
import co.cask.cdap.metrics.process.MetricsConsumerMetaTable;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default implementation of {@link MetricDatasetFactory}, which uses {@link DatasetDefinition} to create
 * {@link MetricsTable} instances.
 */
public class DefaultMetricDatasetFactory implements MetricDatasetFactory {

  private static final Gson GSON = new Gson();

  private final CConfiguration cConf;
  private final DatasetDefinition<MetricsTable, DatasetAdmin> metricsTableDefinition;
  private final Set<DatasetId> existingDatasets;
  private final Supplier<EntityTable> entityTable;

  @Inject
  public DefaultMetricDatasetFactory(CConfiguration cConf,
                                     DatasetDefinition<MetricsTable, DatasetAdmin> metricsTableDefinition) {
    this.cConf = cConf;
    this.metricsTableDefinition = metricsTableDefinition;
    this.existingDatasets = Collections.newSetFromMap(new ConcurrentHashMap<>());
    this.entityTable = Suppliers.memoize(() -> {
      String tableName = cConf.get(Constants.Metrics.ENTITY_TABLE_NAME,
                                   Constants.Metrics.DEFAULT_ENTITY_TABLE_NAME);
      return new EntityTable(getOrCreateMetricsTable(tableName, DatasetProperties.EMPTY));
    });
  }

  // todo: figure out roll time based on resolution from config? See DefaultMetricsTableFactory for example
  @Override
  public FactTable getOrCreateFactTable(int resolution) {
    String tableName = cConf.get(Constants.Metrics.METRICS_TABLE_PREFIX,
                                 Constants.Metrics.DEFAULT_METRIC_TABLE_PREFIX) + ".ts." + resolution;

    TableProperties.Builder props = TableProperties.builder();
    // don't add TTL for MAX_RESOLUTION table. CDAP-1626
    if (resolution != Integer.MAX_VALUE) {
      int ttl = resolution < 60 ? cConf.getInt(Constants.Metrics.MINIMUM_RESOLUTION_RETENTION_SECONDS) :
        cConf.getInt(Constants.Metrics.RETENTION_SECONDS + resolution +
                       Constants.Metrics.RETENTION_SECONDS_SUFFIX);
      if (ttl > 0) {
        props.setTTL(ttl);
      }
    }
    // for efficient counters
    props.setReadlessIncrementSupport(true);
    // configuring pre-splits
    props.add(HBaseTableAdmin.PROPERTY_SPLITS,
              GSON.toJson(FactTable.getSplits(DefaultMetricStore.AGGREGATIONS.size())));
    // Disable auto split
    props.add(HBaseTableAdmin.SPLIT_POLICY,
              cConf.get(Constants.Metrics.METRICS_TABLE_HBASE_SPLIT_POLICY));

    MetricsTable table = getOrCreateResolutionMetricsTable(tableName, props, resolution);
    return new FactTable(table, entityTable.get(), resolution, getRollTime(resolution));
  }

  @Override
  public MetricsConsumerMetaTable createConsumerMeta() {
    String tableName = cConf.get(Constants.Metrics.METRICS_META_TABLE);
    MetricsTable table = getOrCreateMetricsTable(tableName, DatasetProperties.EMPTY);
    return new MetricsConsumerMetaTable(table);
  }

  MetricsTable getOrCreateMetricsTable(String tableName, DatasetProperties props) {
    try {
      // metrics tables are in the system namespace
      return getOrCreateTable(NamespaceId.SYSTEM.dataset(tableName), props);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  MetricsTable getOrCreateResolutionMetricsTable(String tableName, TableProperties.Builder props, int resolution) {
    try {
      props.add(HBaseTableAdmin.PROPERTY_SPLITS,
                GSON.toJson(getMetricsTableSplits(cConf.getInt(Constants.Metrics.METRICS_HBASE_TABLE_SPLITS))));
      props.add(Constants.Metrics.METRICS_HBASE_TABLE_SPLITS,
                cConf.getInt(Constants.Metrics.METRICS_HBASE_TABLE_SPLITS));
      return getOrCreateTable(NamespaceId.SYSTEM.dataset(tableName), props.build());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private MetricsTable getOrCreateTable(DatasetId tableId, DatasetProperties props) throws IOException {
    DatasetContext datasetContext = DatasetContext.from(NamespaceId.SYSTEM.getNamespace());
    DatasetSpecification spec = metricsTableDefinition.configure(tableId.getDataset(), props);

    if (!existingDatasets.contains(tableId)) {
      // Check and create if we don't know if the table exists or not
      DatasetAdmin admin = metricsTableDefinition.getAdmin(datasetContext, spec, getClass().getClassLoader());
      if (!admin.exists()) {
        // All admin.create() implementations handled race condition for concurrent create.
        // Not sure if that's the API contract or just the implementations since it is not specified in the API
        // But from the dataset op executor implementation, it seems it is a required contract.
        admin.create();
      }
      existingDatasets.add(tableId);
    }

    return metricsTableDefinition.getDataset(datasetContext, spec, Collections.emptyMap(), getClass().getClassLoader());
  }

  private static byte[][] getMetricsTableSplits(int splits) {
    RowKeyDistributorByHashPrefix rowKeyDistributor = new RowKeyDistributorByHashPrefix(
      new RowKeyDistributorByHashPrefix.OneByteSimpleHash(splits));
    return rowKeyDistributor.getSplitKeys(splits, splits);
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
