package com.continuuity.runtime;

import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.metrics2.collector.MetricsCollectionServer;
import com.continuuity.metrics2.collector.MetricsCollectionServerInterface;
import com.continuuity.metrics2.frontend.MetricsFrontendServer;
import com.continuuity.metrics2.frontend.MetricsFrontendServerInterface;
import com.google.inject.AbstractModule;
import com.google.inject.Module;

/**
 * Injectable modules related to metric collection service.
 */
public class MetricsModules extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetricsCollectionServerInterface.class)
          .to(MetricsCollectionServer.class);
        bind(MetricsFrontendServerInterface.class)
          .to(MetricsFrontendServer.class);
      }
    };
  }

  @Override
  public Module getSingleNodeModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetricsCollectionServerInterface.class)
          .to(MetricsCollectionServer.class);
        bind(MetricsFrontendServerInterface.class)
          .to(MetricsFrontendServer.class);
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(MetricsCollectionServerInterface.class)
          .to(MetricsCollectionServer.class);
        bind(MetricsFrontendServerInterface.class)
          .to(MetricsFrontendServer.class);
      }
    };
  }
}
