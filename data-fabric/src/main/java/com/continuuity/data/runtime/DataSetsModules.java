package com.continuuity.data.runtime;

import com.continuuity.data2.datafabric.dataset.DataFabricDatasetManager;
import com.continuuity.data2.dataset2.manager.DatasetManager;
import com.continuuity.data2.dataset2.manager.inmemory.DefaultDatasetDefinitionRegistry;
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
        bind(DatasetManager.class).to(DataFabricDatasetManager.class);
        expose(DatasetManager.class);
      }
    };

  }

  public Module getLocalModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(DatasetDefinitionRegistry.class).to(DefaultDatasetDefinitionRegistry.class);
        bind(DatasetManager.class).to(DataFabricDatasetManager.class);
        expose(DatasetManager.class);
      }
    };

  }

  public Module getDistributedModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(DatasetDefinitionRegistry.class).to(DefaultDatasetDefinitionRegistry.class);
        bind(DatasetManager.class).to(DataFabricDatasetManager.class);
        expose(DatasetManager.class);
      }
    };
  }
}
