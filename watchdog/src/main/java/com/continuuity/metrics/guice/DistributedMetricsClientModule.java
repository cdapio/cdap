/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.internal.io.DatumWriter;
import com.continuuity.internal.io.DatumWriterFactory;
import com.continuuity.internal.io.SchemaGenerator;
import com.continuuity.internal.io.UnsupportedTypeException;
import com.continuuity.kafka.client.KafkaClientService;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.collect.KafkaMetricsCollectionService;
import com.continuuity.metrics.transport.MetricsRecord;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.name.Named;

/**
 * Guice module for binding classes for metrics client in distributed runtime mode.
 */
public final class DistributedMetricsClientModule extends PrivateModule {

  private final KafkaClientService kafkaClient;

  DistributedMetricsClientModule(KafkaClientService kafkaClient) {
    this.kafkaClient = kafkaClient;
  }

  @Override
  protected void configure() {
    bind(MetricsCollectionService.class).to(KafkaMetricsCollectionService.class).in(Scopes.SINGLETON);
    expose(MetricsCollectionService.class);
  }

  @Provides
  @Named(MetricsConstants.ConfigKeys.KAFKA_TOPIC_PREFIX)
  public String providesKafkaTopicPrefix(CConfiguration cConf) {
    return cConf.get(MetricsConstants.ConfigKeys.KAFKA_TOPIC_PREFIX, MetricsConstants.DEFAULT_KAFKA_TOPIC_PREFIX);
  }

  @Provides
  public KafkaClientService providesKafkaClient() {
    return kafkaClient;
  }

  @Provides
  public DatumWriter<MetricsRecord> providesDatumWriter(SchemaGenerator schemaGenerator,
                                                        DatumWriterFactory datumWriterFactory) {
    try {
      TypeToken<MetricsRecord> metricRecordType = TypeToken.of(MetricsRecord.class);
      return datumWriterFactory.create(metricRecordType, schemaGenerator.generate(metricRecordType.getType()));
    } catch (UnsupportedTypeException e) {
      throw Throwables.propagate(e);
    }
  }
}
