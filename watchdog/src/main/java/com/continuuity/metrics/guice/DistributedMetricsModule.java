/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.internal.io.ASMDatumWriterFactory;
import com.continuuity.internal.io.ASMFieldAccessorFactory;
import com.continuuity.internal.io.DatumWriter;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.collect.KafkaMetricsCollectionService;
import com.continuuity.metrics.collect.MetricsCollectionService;
import com.continuuity.metrics.data.HBaseFilterableOVCTableHandle;
import com.continuuity.metrics.transport.MetricsRecord;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Named;

/**
 * Guice module for creating bindings for metrics system in distributed mode.
 */
final class DistributedMetricsModule extends AbstractMetricsModule {

  private final ZKClient kafkaZKClient;

  DistributedMetricsModule(ZKClient kafkaZKClient) {
    this.kafkaZKClient = kafkaZKClient;
  }

  @Override
  protected void bindTableHandle() {
    bind(OVCTableHandle.class).to(HBaseFilterableOVCTableHandle.class).in(Scopes.SINGLETON);
    bind(MetricsCollectionService.class).to(KafkaMetricsCollectionService.class).in(Scopes.SINGLETON);
  }

  @Provides
  @Named("metrics.kafka.topic")
  public String providesKafkaTopic(CConfiguration cConf) {
    return cConf.get(MetricsConstants.ConfigKeys.KAFKA_TOPIC, MetricsConstants.DEFAULT_KAFKA_TOPIC);
  }

  @Provides
  public ZKClient providesZKClient() {
    return kafkaZKClient;
  }

  @Provides
  public DatumWriter<MetricsRecord> providesDatumWriter() {
    try {
      TypeToken<MetricsRecord> metricRecordType = TypeToken.of(MetricsRecord.class);
      Schema schema = new ReflectionSchemaGenerator().generate(metricRecordType.getType());
      return new ASMDatumWriterFactory(new ASMFieldAccessorFactory()).create(metricRecordType, schema);
    } catch (UnsupportedTypeException e) {
      throw Throwables.propagate(e);
    }
  }
}

