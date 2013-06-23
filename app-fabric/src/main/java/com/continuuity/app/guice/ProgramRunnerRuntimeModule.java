/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.guice;

import com.continuuity.common.runtime.RuntimeModule;
import com.google.inject.Module;

/**
 *
 */
public final class ProgramRunnerRuntimeModule extends RuntimeModule {

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
    return new DistributedProgramRunnerModule();
  }
}
