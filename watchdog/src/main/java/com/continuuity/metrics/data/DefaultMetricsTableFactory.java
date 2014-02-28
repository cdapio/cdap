/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.dataset.api.DataSetManager;
import com.continuuity.data2.dataset.lib.table.MetricsTable;
import com.continuuity.data2.dataset.lib.table.TimeToLiveSupported;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.process.KafkaConsumerMetaTable;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * Implementation of {@link MetricsTableFactory} that reuses the same instance of {@link MetricsEntityCodec} for
 * creating instances of various type of metrics table.
 */
public final class DefaultMetricsTableFactory implements MetricsTableFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetricsTableFactory.class);

  private final CConfiguration cConf;
  // Stores the MetricsEntityCodec per namespace
  private final LoadingCache<String, MetricsEntityCodec> entityCodecs;
  private final DataSetAccessor accessor;
  private final DataSetManager manager;

  @Inject
  public DefaultMetricsTableFactory(final CConfiguration cConf,
                                    final DataSetAccessor accessor) {
    try {
      this.cConf = cConf;
      this.accessor = accessor;
      this.manager = accessor.getDataSetManager(MetricsTable.class, DataSetAccessor.Namespace.SYSTEM);
      this.entityCodecs = CacheBuilder.newBuilder().build(new CacheLoader<String, MetricsEntityCodec>() {
        @Override
        public MetricsEntityCodec load(String namespace) throws Exception {
          String tableName = namespace.toLowerCase() + "." + cConf.get(MetricsConstants.ConfigKeys.ENTITY_TABLE_NAME,
                                                                       MetricsConstants.DEFAULT_ENTITY_TABLE_NAME);
          accessor.getDataSetManager(MetricsTable.class, DataSetAccessor.Namespace.SYSTEM).create(tableName);
          MetricsTable table = accessor.getDataSetClient(tableName, MetricsTable.class,
                                                         DataSetAccessor.Namespace.SYSTEM);
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

      Properties props = new Properties();
      if (isTTLSupported() && ttl > 0) {
        props.setProperty(TimeToLiveSupported.PROPERTY_TTL, Integer.toString(ttl));
      }
      manager.create(tableName, props);
      MetricsTable table = accessor.getDataSetClient(tableName, MetricsTable.class, DataSetAccessor.Namespace.SYSTEM);
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
      manager.create(tableName);
      MetricsTable table = accessor.getDataSetClient(tableName, MetricsTable.class, DataSetAccessor.Namespace.SYSTEM);
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
      manager.create(tableName);
      MetricsTable table = accessor.getDataSetClient(tableName, MetricsTable.class, DataSetAccessor.Namespace.SYSTEM);
      LOG.info("KafkaConsumerMetaTable created: {}", tableName);
      return new KafkaConsumerMetaTable(table);
    } catch (Exception e) {
      LOG.error("Exception in creating KafkaConsumerMetaTable.", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean isTTLSupported() {
    return (manager instanceof TimeToLiveSupported) && ((TimeToLiveSupported) manager).isTTLSupported();
  }

  @Override
  public void upgrade() throws Exception {
    String metricsPrefix = cConf.get(MetricsConstants.ConfigKeys.METRICS_TABLE_PREFIX,
                                     MetricsConstants.DEFAULT_METRIC_TABLE_PREFIX);
    for (Map.Entry<String, Class<?>> entry : accessor.list(DataSetAccessor.Namespace.SYSTEM).entrySet()) {
      String tableName = entry.getKey();
      // See if it is timeseries or aggregates table

      if (tableName.contains(metricsPrefix + ".ts.")) {
        // Time series
        // Parse the time resolution
        int resolution = Integer.parseInt(tableName.substring(tableName.lastIndexOf('.') + 1));
        int ttl =  cConf.getInt(MetricsConstants.ConfigKeys.RETENTION_SECONDS + "." + resolution + ".seconds", -1);
        Properties props = new Properties();
        if (isTTLSupported() && ttl > 0) {
          props.setProperty(TimeToLiveSupported.PROPERTY_TTL, Integer.toString(ttl));
        }
        manager.upgrade(tableName, props);
      } else if (tableName.contains(metricsPrefix + ".agg")) {
        // Aggregate
        manager.upgrade(tableName, new Properties());
      }
    }
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
