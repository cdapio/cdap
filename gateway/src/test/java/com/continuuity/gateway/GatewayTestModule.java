/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.gateway;

import com.continuuity.app.guice.AppFabricServiceRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.app.guice.ProgramRunnerRuntimeModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.google.inject.AbstractModule;

/**
 *
 */
public final class GatewayTestModule extends AbstractModule {

  private final CConfiguration configuration;

  public GatewayTestModule(CConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected void configure() {
    install(new DataFabricModules().getInMemoryModules());
    install(new ConfigModule(configuration));
    install(new IOModule());
    install(new MetricsClientRuntimeModule().getNoopModules());
    install(new DiscoveryRuntimeModule().getInMemoryModules());
    install(new LocationRuntimeModule().getInMemoryModules());
    install(new AppFabricServiceRuntimeModule().getInMemoryModules());
    install(new ProgramRunnerRuntimeModule().getInMemoryModules());
  }
}
