package com.continuuity.data.runtime;

import com.continuuity.data2.datafabric.dataset.RemoteDatasetFramework;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.DefaultDatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.google.inject.Module;
import com.google.inject.PrivateModule;

/**
 * DataSets framework bindings
 */
class DataSetsModules {
  public Module getInMemoryModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(DatasetDefinitionRegistry.class).to(DefaultDatasetDefinitionRegistry.class);
        bind(DatasetFramework.class).to(RemoteDatasetFramework.class);
        expose(DatasetFramework.class);
      }
    };

  }

  public Module getLocalModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(DatasetDefinitionRegistry.class).to(DefaultDatasetDefinitionRegistry.class);
        bind(DatasetFramework.class).to(RemoteDatasetFramework.class);
        expose(DatasetFramework.class);
      }
    };

  }

  public Module getDistributedModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(DatasetDefinitionRegistry.class).to(DefaultDatasetDefinitionRegistry.class);
        bind(DatasetFramework.class).to(RemoteDatasetFramework.class);
        expose(DatasetFramework.class);
      }
    };
  }
}
