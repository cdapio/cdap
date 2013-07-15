/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.guice;

import com.continuuity.api.metrics.MetricsCollectionService;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.kafka.client.KafkaClientService;
import com.continuuity.metrics.collect.LocalMetricsCollectionService;
import com.google.common.base.Preconditions;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;

/**
 *
 */
public final class MetricsClientRuntimeModule extends RuntimeModule {

  private final KafkaClientService kafkaClient;

  public MetricsClientRuntimeModule() {
    this(null);
  }

  public MetricsClientRuntimeModule(KafkaClientService kafkaClient) {
    this.kafkaClient = kafkaClient;
  }

  @Override
  public Module getInMemoryModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        install(new InMemoryMetricsTableModule());
        install(new MetricsProcessorModule());
        bind(MetricsCollectionService.class).to(LocalMetricsCollectionService.class).in(Scopes.SINGLETON);
        expose(MetricsCollectionService.class);
      }
    };
  }

  @Override
  public Module getSingleNodeModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        install(new LocalMetricsTableModule());
        install(new MetricsProcessorModule());
        bind(MetricsCollectionService.class).to(LocalMetricsCollectionService.class).in(Scopes.SINGLETON);
        expose(MetricsCollectionService.class);
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    Preconditions.checkNotNull(kafkaClient, "Kafka client cannot be null for distributed module.");
    return new DistributedMetricsClientModule(kafkaClient);
  }
}
