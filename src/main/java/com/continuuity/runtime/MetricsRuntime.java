package com.continuuity.runtime;

import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.metrics.service.*;
import com.continuuity.observer.StateChangeCallback;
import com.continuuity.observer.internal.SQLStateChangeSyncer;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.name.Names;

/**
 *
 */
public class MetricsRuntime extends RuntimeModule {
  @Override
  public Module getInMemory() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetricsServer.class).to(MetricsSingleNodeServer.class);
        bind(MetricsHandler.class).to(SQLMetricsHandler.class);
        bind(StateChangeCallback.class).to(SQLStateChangeSyncer.class);
        bind(String.class).annotatedWith(Names.named("Flow Monitor JDBC URL")).toInstance("jdbc:hsqldb:mem:fmdb");
      }
    };
  }

  @Override
  public Module getSingleNode() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetricsServer.class).to(MetricsSingleNodeServer.class);
        bind(MetricsHandler.class).to(SQLMetricsHandler.class);
        bind(StateChangeCallback.class).to(SQLStateChangeSyncer.class);
        bind(String.class).annotatedWith(Names.named("Flow Monitor JDBC URL")).toInstance("jdbc:hsqldb:file:/tmp/data/flowmonitordb");
      }
    };
  }

  @Override
  public Module getDistributed() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetricsServer.class).to(MetricsRegisteredServer.class);
        bind(MetricsHandler.class).to(SQLMetricsHandler.class);
        bind(StateChangeCallback.class).to(SQLStateChangeSyncer.class);
        bind(String.class).annotatedWith(Names.named("Flow Monitor JDBC URL")).toInstance("jdbc:hsqldb:file:data/flowmonitordb");
      }
    };
  }
}
