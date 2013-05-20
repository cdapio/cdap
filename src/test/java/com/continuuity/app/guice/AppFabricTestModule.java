/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.inject.AbstractModule;

/**
 *
 */
public final class AppFabricTestModule extends AbstractModule {

  private final CConfiguration configuration;

  public AppFabricTestModule(CConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected void configure() {
    install(new DataFabricModules().getInMemoryModules());
    install(new ConfigModule(configuration));
    install(new IOModule());
    install(new DiscoveryRuntimeModule().getInMemoryModules());
    install(new LocationRuntimeModule().getInMemoryModules());
    install(new AppFabricServiceRuntimeModule().getInMemoryModules());
    install(new ProgramRunnerRuntimeModule().getInMemoryModules());
  }
}
