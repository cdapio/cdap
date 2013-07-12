/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.guice;

import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.base.Preconditions;
import com.google.inject.Module;

/**
 * The {@link RuntimeModule} for configuring metrics bindings in different runtime environments.
 */
public final class MetricsRuntimeModule extends RuntimeModule {

  private final ZKClient kafkaZKClient;

  public MetricsRuntimeModule() {
    this(null);
  }

  public MetricsRuntimeModule(ZKClient kafkaZkClient) {
    this.kafkaZKClient = kafkaZkClient;
  }

  @Override
  public Module getInMemoryModules() {
    return new LocalMetricsModule();
  }

  @Override
  public Module getSingleNodeModules() {
    return new LocalMetricsModule();
  }

  @Override
  public Module getDistributedModules() {
    Preconditions.checkState(kafkaZKClient != null, "No ZKClient provided for connecting with kafka.");
    return new DistributedMetricsModule(kafkaZKClient);
  }
}
