/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.metrics.data;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.table.OrderedTable;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.hbase.HBaseMetricsTable;
import co.cask.cdap.metrics.MetricsConstants;
import co.cask.cdap.metrics.process.KafkaConsumerMetaTable;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Implementation of {@link MetricsTableFactory} that reuses the same instance of {@link MetricsEntityCodec} for
 * creating instances of various type of metrics table.
 */
public final class DefaultMetricsTableFactory implements MetricsTableFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetricsTableFactory.class);

  private final CConfiguration cConf;
  // Stores the MetricsEntityCodec per namespace
  private final LoadingCache<String, MetricsEntityCodec> entityCodecs;
  private final DatasetFramework dsFramework;

  private Boolean ttlSupported;

  @Inject
  public DefaultMetricsTableFactory(final CConfiguration cConf,
                                    final DatasetFramework dsFramework) {
    try {
      this.cConf = cConf;
      this.dsFramework =
        new NamespacedDatasetFramework(dsFramework,
                                       new DefaultDatasetNamespace(cConf, Namespace.SYSTEM));

      this.entityCodecs = CacheBuilder.newBuilder().build(new CacheLoader<String, MetricsEntityCodec>() {
        @Override
        public MetricsEntityCodec load(String namespace) throws Exception {
          String tableName = namespace.toLowerCase() + "." + cConf.get(MetricsConstants.ConfigKeys.ENTITY_TABLE_NAME,
                                                                       MetricsConstants.DEFAULT_ENTITY_TABLE_NAME);
          // get the TTL of hour time-series table and use that as TTL for entity table.
          int ttl =  cConf.getInt(MetricsConstants.ConfigKeys.RETENTION_SECONDS + ".3600.seconds", -1);
          DatasetProperties props = ttl > 0 ?
            DatasetProperties.builder().add(OrderedTable.PROPERTY_TTL, ttl).build() : DatasetProperties.EMPTY;
          MetricsTable table = getOrCreateMetricsTable(tableName, props);
          EntityTable entityTable = new EntityTable(table);

          return new MetricsEntityCodec(entityTable,
                                        MetricsConstants.DEFAULT_CONTEXT_DEPTH,
                                        MetricsConstants.DEFAULT_METRIC_DEPTH,
                                        MetricsConstants.DEFAULT_TAG_DEPTH);
        }
      });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public TimeSeriesTable createTimeSeries(String namespace, int resolution) {
    try {
      String tableName = namespace.toLowerCase() + "." +
                          cConf.get(MetricsConstants.ConfigKeys.METRICS_TABLE_PREFIX,
                                    MetricsConstants.DEFAULT_METRIC_TABLE_PREFIX) + ".ts." + resolution;
      int ttl =  cConf.getInt(MetricsConstants.ConfigKeys.RETENTION_SECONDS + "." + resolution + ".seconds", -1);
      DatasetProperties props = ttl > 0 ?
        DatasetProperties.builder().add(OrderedTable.PROPERTY_TTL, ttl).build() : DatasetProperties.EMPTY;
      MetricsTable table = getOrCreateMetricsTable(tableName, props);
      LOG.info("TimeSeriesTable created: {}", tableName);
      return new TimeSeriesTable(table, entityCodecs.getUnchecked(namespace), resolution, getRollTime(resolution));
    } catch (Exception e) {
      LOG.error("Exception in creating TimeSeriesTable.", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public AggregatesTable createAggregates(String namespace) {
    try {
      String tableName = namespace.toLowerCase() + "." +
                          cConf.get(MetricsConstants.ConfigKeys.METRICS_TABLE_PREFIX,
                                    MetricsConstants.DEFAULT_METRIC_TABLE_PREFIX) + ".agg";
      MetricsTable table = getOrCreateMetricsTable(tableName, DatasetProperties.EMPTY);
      LOG.info("AggregatesTable created: {}", tableName);
      return new AggregatesTable(table, entityCodecs.getUnchecked(namespace));
    } catch (Exception e) {
      LOG.error("Exception in creating AggregatesTable.", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public KafkaConsumerMetaTable createKafkaConsumerMeta(String namespace) {
    try {
      String tableName = namespace.toLowerCase() + "." + cConf.get(MetricsConstants.ConfigKeys.KAFKA_META_TABLE,
                                                     MetricsConstants.DEFAULT_KAFKA_META_TABLE);
      MetricsTable table = getOrCreateMetricsTable(tableName, DatasetProperties.EMPTY);
      LOG.info("KafkaConsumerMetaTable created: {}", tableName);
      return new KafkaConsumerMetaTable(table);
    } catch (Exception e) {
      LOG.error("Exception in creating KafkaConsumerMetaTable.", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean isTTLSupported() {
    if (ttlSupported == null) {
      // this is pretty dirty hack: we know that only HBaseMetricsTable supports TTL
      // todo: expose type information in different way
      try {
        ttlSupported = dsFramework.hasType(HBaseMetricsTable.class.getName());
      } catch (DatasetManagementException e) {
        throw Throwables.propagate(e);
      }
    }
    return ttlSupported;
  }

  @Override
  public void upgrade() throws Exception {
    String metricsPrefix = cConf.get(MetricsConstants.ConfigKeys.METRICS_TABLE_PREFIX,
                                     MetricsConstants.DEFAULT_METRIC_TABLE_PREFIX);
    for (DatasetSpecification spec : dsFramework.getInstances()) {
      String dsName = spec.getName();
      // See if it is timeseries or aggregates table

      if (dsName.contains(metricsPrefix + ".ts.") || dsName.contains(metricsPrefix + ".agg")) {
        DatasetAdmin admin = dsFramework.getAdmin(dsName, null);
        if (admin != null) {
          admin.upgrade();
        } else {
          LOG.error("Could not obtain admin to upgrade metrics table: " + dsName);
          // continue to best effort
        }
      }
    }
  }

  private MetricsTable getOrCreateMetricsTable(String tableName, DatasetProperties props)
    throws DatasetManagementException, IOException {

    return DatasetsUtil.getOrCreateDataset(dsFramework, tableName, MetricsTable.class.getName(), props, null, null);
  }

  private int getRollTime(int resolution) {
    String key = MetricsConstants.ConfigKeys.TIME_SERIES_TABLE_ROLL_TIME + "." + resolution;
    String value = cConf.get(key);
    if (value != null) {
      return cConf.getInt(key, MetricsConstants.DEFAULT_TIME_SERIES_TABLE_ROLL_TIME);
    }
    return cConf.getInt(MetricsConstants.ConfigKeys.TIME_SERIES_TABLE_ROLL_TIME,
                        MetricsConstants.DEFAULT_TIME_SERIES_TABLE_ROLL_TIME);
  }
}
