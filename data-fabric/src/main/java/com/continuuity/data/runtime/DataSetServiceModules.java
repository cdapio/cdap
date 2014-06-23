package com.continuuity.data.runtime;

import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.datafabric.dataset.service.DatasetService;
import com.continuuity.data2.datafabric.dataset.service.executor.DatasetAdminOpHTTPHandler;
import com.continuuity.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import com.continuuity.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import com.continuuity.data2.datafabric.dataset.service.executor.InMemoryDatasetOpExecutor;
import com.continuuity.data2.datafabric.dataset.service.executor.LocalDatasetOpExecutor;
import com.continuuity.data2.datafabric.dataset.service.executor.YarnDatasetOpExecutor;
import com.continuuity.data2.datafabric.dataset.service.mds.MDSDatasetsRegistry;
import com.continuuity.data2.dataset2.DatasetDefinitionRegistryFactory;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.DefaultDatasetDefinitionRegistry;
import com.continuuity.data2.dataset2.InMemoryDatasetFramework;
import com.continuuity.data2.dataset2.lib.table.CoreDatasetsModule;
import com.continuuity.data2.dataset2.module.lib.hbase.HBaseOrderedTableModule;
import com.continuuity.data2.dataset2.module.lib.inmemory.InMemoryOrderedTableModule;
import com.continuuity.data2.dataset2.module.lib.leveldb.LevelDBOrderedTableModule;
import com.continuuity.gateway.handlers.PingHandler;
import com.continuuity.http.HttpHandler;
import com.google.common.collect.Maps;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

import java.util.Map;

/**
 * Bindings for DataSet Service.
 */
public class DataSetServiceModules {
  public Module getInMemoryModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        // NOTE: order is important due to dependencies between modules
        Map<String, DatasetModule> defaultModules = Maps.newLinkedHashMap();
        defaultModules.put("orderedTable-memory", new InMemoryOrderedTableModule());
        defaultModules.put("core", new CoreDatasetsModule());

        bind(new TypeLiteral<Map<String, ? extends DatasetModule>>() { })
          .annotatedWith(Names.named("defaultDatasetModules")).toInstance(defaultModules);

        install(new FactoryModuleBuilder()
                  .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                  .build(DatasetDefinitionRegistryFactory.class));
        // NOTE: it is fine to use in-memory dataset manager for direct access to dataset MDS even in distributed mode
        //       as long as the data is durably persisted
        bind(DatasetFramework.class).annotatedWith(Names.named("datasetMDS")).to(InMemoryDatasetFramework.class);
        bind(MDSDatasetsRegistry.class).in(Singleton.class);
        bind(DatasetService.class);
        expose(DatasetService.class);

        bind(DatasetOpExecutor.class).to(InMemoryDatasetOpExecutor.class);
        expose(DatasetOpExecutor.class);
      }
    };

  }

  public Module getLocalModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        // NOTE: order is important due to dependencies between modules
        Map<String, DatasetModule> defaultModules = Maps.newLinkedHashMap();
        defaultModules.put("orderedTable-leveldb", new LevelDBOrderedTableModule());
        defaultModules.put("core", new CoreDatasetsModule());

        bind(new TypeLiteral<Map<String, ? extends DatasetModule>>() { })
          .annotatedWith(Names.named("defaultDatasetModules")).toInstance(defaultModules);

        install(new FactoryModuleBuilder()
                  .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                  .build(DatasetDefinitionRegistryFactory.class));
        // NOTE: it is fine to use in-memory dataset manager for direct access to dataset MDS even in distributed mode
        //       as long as the data is durably persisted
        bind(DatasetFramework.class).annotatedWith(Names.named("datasetMDS")).to(InMemoryDatasetFramework.class);
        bind(MDSDatasetsRegistry.class).in(Singleton.class);
        bind(DatasetService.class);
        expose(DatasetService.class);

        Named datasetUserName = Names.named(Constants.Service.DATASET_EXECUTOR);
        Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class, datasetUserName);
        handlerBinder.addBinding().to(DatasetAdminOpHTTPHandler.class);
        handlerBinder.addBinding().to(PingHandler.class);

        bind(DatasetOpExecutorService.class).in(Scopes.SINGLETON);
        expose(DatasetOpExecutorService.class);

        bind(DatasetOpExecutor.class).to(LocalDatasetOpExecutor.class);
        expose(DatasetOpExecutor.class);
      }
    };

  }

  public Module getDistributedModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        // NOTE: order is important due to dependencies between modules
        Map<String, DatasetModule> defaultModules = Maps.newLinkedHashMap();
        defaultModules.put("orderedTable-hbase", new HBaseOrderedTableModule());
        defaultModules.put("core", new CoreDatasetsModule());

        bind(new TypeLiteral<Map<String, ? extends DatasetModule>>() { })
          .annotatedWith(Names.named("defaultDatasetModules")).toInstance(defaultModules);

        install(new FactoryModuleBuilder()
                  .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                  .build(DatasetDefinitionRegistryFactory.class));
        // NOTE: it is fine to use in-memory dataset manager for direct access to dataset MDS even in distributed mode
        //       as long as the data is durably persisted
        bind(DatasetFramework.class).annotatedWith(Names.named("datasetMDS")).to(InMemoryDatasetFramework.class);
        bind(MDSDatasetsRegistry.class).in(Singleton.class);
        bind(DatasetService.class);
        expose(DatasetService.class);

        Named datasetUserName = Names.named(Constants.Service.DATASET_EXECUTOR);
        Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class, datasetUserName);
        handlerBinder.addBinding().to(DatasetAdminOpHTTPHandler.class);
        handlerBinder.addBinding().to(PingHandler.class);

        bind(DatasetOpExecutorService.class).in(Scopes.SINGLETON);
        expose(DatasetOpExecutorService.class);

        bind(DatasetOpExecutor.class).to(YarnDatasetOpExecutor.class);
        expose(DatasetOpExecutor.class);
      }
    };
  }
}
