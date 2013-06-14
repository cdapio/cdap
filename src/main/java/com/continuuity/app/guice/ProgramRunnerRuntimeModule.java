/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.guice;

import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.base.Preconditions;
import com.google.inject.Module;

/**
 *
 */
public final class ProgramRunnerRuntimeModule extends RuntimeModule {

  private final ZKClient zkClient;

  public ProgramRunnerRuntimeModule() {
    this(null);
  }

  public ProgramRunnerRuntimeModule(ZKClient zkClient) {
    this.zkClient = zkClient;
  }

  @Override
  public Module getInMemoryModules() {
    return new InMemoryProgramRunnerModule();
  }

  @Override
  public Module getSingleNodeModules() {
    return new InMemoryProgramRunnerModule();
  }

  @Override
  public Module getDistributedModules() {
    Preconditions.checkArgument(zkClient != null, "No ZKClient provided.");
    return new DistributedProgramRunnerModule(zkClient);
  }
}
