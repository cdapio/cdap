/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.guice;

import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.metrics.collect.AggregatedMetricsCollectionService;
import com.continuuity.metrics.collect.LocalMetricsCollectionService;
import com.continuuity.metrics.collect.MapReduceCounterCollectionService;
import com.continuuity.metrics.data.DefaultMetricsTableFactory;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.continuuity.metrics.transport.MetricsRecord;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.util.Iterator;

/**
 *
 */
public final class MetricsClientRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        install(new MetricsProcessorModule());
        bind(MetricsTableFactory.class).to(DefaultMetricsTableFactory.class).in(Scopes.SINGLETON);
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
        install(new MetricsProcessorModule());
        bind(MetricsTableFactory.class).to(DefaultMetricsTableFactory.class).in(Scopes.SINGLETON);
        bind(MetricsCollectionService.class).to(LocalMetricsCollectionService.class).in(Scopes.SINGLETON);
        expose(MetricsCollectionService.class);
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    return new DistributedMetricsClientModule();
  }

  public Module getMapReduceModules(final TaskAttemptContext taskContext) {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(TaskAttemptContext.class).toInstance(taskContext);
        bind(MetricsCollectionService.class).to(MapReduceCounterCollectionService.class).in(Scopes.SINGLETON);
        expose(MetricsCollectionService.class);
      }
    };
  }

  /**
   * Returns a module that bind MetricsCollectionService to a noop one.
   */
  public Module getNoopModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetricsCollectionService.class).toInstance(new AggregatedMetricsCollectionService() {
          @Override
          protected void publish(MetricsScope scope, Iterator<MetricsRecord> metrics) throws Exception {
            // No-op
          }
        });
      }
    };
  }
}
