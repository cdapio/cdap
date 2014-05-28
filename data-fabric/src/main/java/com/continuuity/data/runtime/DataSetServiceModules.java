package com.continuuity.data.runtime;

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
 * Bindings for DataSet Service.
 */
public class DataSetServiceModules {
  public Module getInMemoryModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(new TypeLiteral<SortedMap<String, Class<? extends DatasetModule>>>() { })
          .annotatedWith(Names.named("defaultDatasetModules")).toInstance(
          ImmutableSortedMap.<String, Class<? extends DatasetModule>>of(
            "orderedTable-memory", InMemoryTableModule.class,
            "table", TableModule.class)
        );

        bind(DatasetDefinitionRegistry.class).to(DefaultDatasetDefinitionRegistry.class);
        // NOTE: it is fine to use in-memory dataset manager for direct access to dataset MDS even in distributed mode
        //       as long as the data is durably persisted
        bind(DatasetManager.class).annotatedWith(Names.named("datasetMDS")).to(InMemoryDatasetManager.class);
        bind(DatasetManagerService.class);

        expose(DatasetManagerService.class);
      }
    };

  }

  public Module getLocalModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(new TypeLiteral<SortedMap<String, Class<? extends DatasetModule>>>() { })
          .annotatedWith(Names.named("defaultDatasetModules")).toInstance(
          ImmutableSortedMap.<String, Class<? extends DatasetModule>>of(
            "orderedTable-memory", LevelDBTableModule.class,
            "table", TableModule.class)
        );
        bind(DatasetDefinitionRegistry.class).to(DefaultDatasetDefinitionRegistry.class);
        // NOTE: it is fine to use in-memory dataset manager for direct access to dataset MDS even in distributed mode
        //       as long as the data is durably persisted
        bind(DatasetManager.class).annotatedWith(Names.named("datasetMDS")).to(InMemoryDatasetManager.class);
        bind(DatasetManagerService.class);

        expose(DatasetManagerService.class);
      }
    };

  }

  public Module getDistributedModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(new TypeLiteral<SortedMap<String, Class<? extends DatasetModule>>>() { })
          .annotatedWith(Names.named("defaultDatasetModules")).toInstance(
          ImmutableSortedMap.<String, Class<? extends DatasetModule>>of(
            "orderedTable-hbase", HBaseTableModule.class,
            "table", TableModule.class)
        );
        bind(DatasetDefinitionRegistry.class).to(DefaultDatasetDefinitionRegistry.class);
        // NOTE: it is fine to use in-memory dataset manager for direct access to dataset MDS even in distributed mode
        //       as long as the data is durably persisted
        bind(DatasetManager.class).annotatedWith(Names.named("datasetMDS")).to(InMemoryDatasetManager.class);
        bind(DatasetManagerService.class);

        expose(DatasetManagerService.class);
      }
    };
  }
}
