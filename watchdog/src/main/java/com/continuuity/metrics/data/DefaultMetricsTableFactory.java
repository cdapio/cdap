/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.data.table.OrderedVersionedColumnarTable;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.guice.MetricsAnnotation;
import com.continuuity.metrics.process.KafkaConsumerMetaTable;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link MetricsTableFactory} that reuses the same instance of {@link MetricsEntityCodec} for
 * creating instances of various type of metrics table.
 */
public final class DefaultMetricsTableFactory implements MetricsTableFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetricsTableFactory.class);

  private final CConfiguration cConf;
  private final OVCTableHandle tableHandle;
  // Stores the MetricsEntityCodec per namespace
  private final LoadingCache<String, MetricsEntityCodec> entityCodecs;

  @Inject
  public DefaultMetricsTableFactory(final CConfiguration cConf, @MetricsAnnotation final OVCTableHandle tableHandle) {
    this.cConf = cConf;
    this.tableHandle = tableHandle;
    this.entityCodecs = CacheBuilder.newBuilder().build(new CacheLoader<String, MetricsEntityCodec>() {
      @Override
      public MetricsEntityCodec load(String namespace) throws Exception {
        String tableName = namespace + "." + cConf.get(MetricsConstants.ConfigKeys.ENTITY_TABLE_NAME,
                                                       MetricsConstants.DEFAULT_ENTITY_TABLE_NAME);
        EntityTable entityTable = new EntityTable(tableHandle.getTable(Bytes.toBytes(tableName)));

        return new MetricsEntityCodec(entityTable,
                                      MetricsConstants.DEFAULT_CONTEXT_DEPTH,
                                      MetricsConstants.DEFAULT_METRIC_DEPTH,
                                      MetricsConstants.DEFAULT_TAG_DEPTH);
      }
    });
  }

  @Override
  public TimeSeriesTable createTimeSeries(String namespace, int resolution) {
    try {
      String tableName = namespace + "." +
                          cConf.get(MetricsConstants.ConfigKeys.METRICS_TABLE_PREFIX,
                                    MetricsConstants.DEFAULT_METRIC_TABLE_PREFIX) + ".ts." + resolution;
      int ttl =  cConf.getInt(MetricsConstants.ConfigKeys.RETENTION_SECONDS + "." + resolution + ".seconds", -1);

      OrderedVersionedColumnarTable table;
      if (ttl > 0 && tableHandle instanceof TimeToLiveOVCTableHandle) {
        // If TTL exists and the table handle supports it, use the TTL as well.
        table = ((TimeToLiveOVCTableHandle) tableHandle).getTable(Bytes.toBytes(tableName), ttl);
      } else {
        table = tableHandle.getTable(Bytes.toBytes(tableName));
      }

      LOG.info("TimeSeriesTable created: {}", tableName);
      return new TimeSeriesTable(table, entityCodecs.getUnchecked(namespace), resolution, getRollTime(resolution));
    } catch (OperationException e) {
      LOG.error("Exception in creating TimeSeriesTable.", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public AggregatesTable createAggregates(String namespace) {
    try {
      String tableName = namespace + "." +
                          cConf.get(MetricsConstants.ConfigKeys.METRICS_TABLE_PREFIX,
                                    MetricsConstants.DEFAULT_METRIC_TABLE_PREFIX) + ".agg";

      LOG.info("AggregatesTable created: {}", tableName);
      return new AggregatesTable(tableHandle.getTable(Bytes.toBytes(tableName)), entityCodecs.getUnchecked(namespace));
    } catch (OperationException e) {
      LOG.error("Exception in creating AggregatesTable.", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public KafkaConsumerMetaTable createKafkaConsumerMeta(String namespace) {
    try {
      String tableName = namespace + "." + cConf.get(MetricsConstants.ConfigKeys.KAFKA_META_TABLE,
                                                     MetricsConstants.DEFAULT_KAFKA_META_TABLE);

      LOG.info("KafkaConsumerMetaTable created: {}", tableName);
      return new KafkaConsumerMetaTable(tableHandle.getTable(Bytes.toBytes(tableName)));
    } catch (OperationException e) {
      LOG.error("Exception in creating KafkaConsumerMetaTable.", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean isTTLSupported() {
    return tableHandle instanceof TimeToLiveOVCTableHandle;
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
