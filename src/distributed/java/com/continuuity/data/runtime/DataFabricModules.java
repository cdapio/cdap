/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.runtime;

import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Singleton;

/**
 * DataFabricModules defines all of the bindings for the different data
 * fabric modes.
 */
public class DataFabricModules extends RuntimeModule {

  public Module getNoopModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(OperationExecutor.class).
            to(NoOperationExecutor.class).in(Singleton.class);
      }
    };
  }

  @Override
  public Module getInMemoryModules() {
    return new DataFabricMemoryModule();
  }

  @Override
  public Module getSingleNodeModules() {
    return new DataFabricLocalModule();
  }

  @Override
  public Module getDistributedModules() {
    return new DataFabricDistributedModule();
  }

} // end of DataFabricModules
