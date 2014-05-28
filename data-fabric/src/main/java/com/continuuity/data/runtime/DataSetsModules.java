package com.continuuity.data.runtime;

import com.continuuity.data2.datafabric.dataset.DataFabricDatasetManager;
import com.continuuity.data2.datafabric.dataset.service.DatasetManagerService;
import com.continuuity.data2.dataset2.manager.DatasetManager;
import com.continuuity.data2.dataset2.manager.inmemory.DefaultDatasetDefinitionRegistry;
import com.continuuity.data2.dataset2.manager.inmemory.InMemoryDatasetManager;
import com.continuuity.data2.dataset2.module.lib.TableModule;
import com.continuuity.data2.dataset2.module.lib.hbase.HBaseTableModule;
import com.continuuity.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import com.continuuity.data2.dataset2.module.lib.leveldb.LevelDBTableModule;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;
import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import java.util.SortedMap;

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
