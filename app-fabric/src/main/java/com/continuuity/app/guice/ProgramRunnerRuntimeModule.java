/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.guice;

import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.gateway.auth.GatewayAuthModules;
import com.continuuity.gateway.handlers.AppFabricGatewayModules;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.util.Modules;

/**
 *
 */
public final class ProgramRunnerRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return Modules.combine(new InMemoryProgramRunnerModule(),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               install(new GatewayAuthModules());
                               install(new AppFabricGatewayModules());
                             }
                           });
  }

  @Override
  public Module getSingleNodeModules() {
    return Modules.combine(new InMemoryProgramRunnerModule(),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               install(new GatewayAuthModules());
                               install(new AppFabricGatewayModules());
                             }
                           });
  }

  @Override
  public Module getDistributedModules() {
    // Note: In distributed mode, the gateway modules are bound only in the program runner that needs it.
    return new DistributedProgramRunnerModule();
  }
}
