/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.guice;

import com.continuuity.common.runtime.RuntimeModule;
import com.google.inject.Module;

/**
 *
 */
public class MetricsQueryRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new AbstractMetricsQueryModule() {
      @Override
      protected void bindMetricsTable() {
        install(new InMemoryMetricsTableModule());
      }
    };
  }

  @Override
  public Module getSingleNodeModules() {
    return new AbstractMetricsQueryModule() {
      @Override
      protected void bindMetricsTable() {
        install(new LocalMetricsTableModule());
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    return new AbstractMetricsQueryModule() {
      @Override
      protected void bindMetricsTable() {
        install(new DistributedMetricsTableModule());
      }
    };
  }
}
