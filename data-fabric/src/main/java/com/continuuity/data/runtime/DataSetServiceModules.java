package com.continuuity.data.runtime;

import com.continuuity.common.conf.Constants;
import com.continuuity.data2.datafabric.dataset.service.DatasetService;
import com.continuuity.data2.datafabric.dataset.service.executor.DatasetAdminOpHTTPHandler;
import com.continuuity.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import com.continuuity.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import com.continuuity.data2.datafabric.dataset.service.executor.InMemoryDatasetOpExecutor;
import com.continuuity.data2.datafabric.dataset.service.executor.LocalDatasetOpExecutor;
import com.continuuity.data2.datafabric.dataset.service.executor.YarnDatasetOpExecutor;
import com.continuuity.data2.dataset2.DatasetFramework;
import com.continuuity.data2.dataset2.DefaultDatasetDefinitionRegistry;
import com.continuuity.data2.dataset2.InMemoryDatasetFramework;
import com.continuuity.data2.dataset2.module.lib.TableModule;
import com.continuuity.data2.dataset2.module.lib.hbase.HBaseTableModule;
import com.continuuity.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import com.continuuity.data2.dataset2.module.lib.leveldb.LevelDBTableModule;
import com.continuuity.gateway.handlers.PingHandler;
import com.continuuity.http.HttpHandler;
import com.continuuity.internal.data.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.internal.data.dataset.module.DatasetModule;
import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
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
        bind(DatasetFramework.class).annotatedWith(Names.named("datasetMDS")).to(InMemoryDatasetFramework.class);
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
        bind(new TypeLiteral<SortedMap<String, Class<? extends DatasetModule>>>() { })
          .annotatedWith(Names.named("defaultDatasetModules")).toInstance(
          ImmutableSortedMap.<String, Class<? extends DatasetModule>>of(
            "orderedTable-memory", LevelDBTableModule.class,
            "table", TableModule.class)
        );
        bind(DatasetDefinitionRegistry.class).to(DefaultDatasetDefinitionRegistry.class);
        // NOTE: it is fine to use in-memory dataset manager for direct access to dataset MDS even in distributed mode
        //       as long as the data is durably persisted
        bind(DatasetFramework.class).annotatedWith(Names.named("datasetMDS")).to(InMemoryDatasetFramework.class);
        bind(DatasetService.class);
        expose(DatasetService.class);

        Named datasetUserName = Names.named(Constants.Service.DATASET_EXECUTOR);
        Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class, datasetUserName);
        handlerBinder.addBinding().to(DatasetAdminOpHTTPHandler.class);
        handlerBinder.addBinding().to(PingHandler.class);

        bind(DatasetDefinitionRegistry.class).to(DefaultDatasetDefinitionRegistry.class);
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
        bind(new TypeLiteral<SortedMap<String, Class<? extends DatasetModule>>>() { })
          .annotatedWith(Names.named("defaultDatasetModules")).toInstance(
          ImmutableSortedMap.<String, Class<? extends DatasetModule>>of(
            "orderedTable-hbase", HBaseTableModule.class,
            "table", TableModule.class)
        );
        bind(DatasetDefinitionRegistry.class).to(DefaultDatasetDefinitionRegistry.class);
        // NOTE: it is fine to use in-memory dataset manager for direct access to dataset MDS even in distributed mode
        //       as long as the data is durably persisted
        bind(DatasetFramework.class).annotatedWith(Names.named("datasetMDS")).to(InMemoryDatasetFramework.class);
        bind(DatasetService.class);
        expose(DatasetService.class);

        Named datasetUserName = Names.named(Constants.Service.DATASET_EXECUTOR);
        Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class, datasetUserName);
        handlerBinder.addBinding().to(DatasetAdminOpHTTPHandler.class);
        handlerBinder.addBinding().to(PingHandler.class);

        bind(DatasetDefinitionRegistry.class).to(DefaultDatasetDefinitionRegistry.class);
        bind(DatasetOpExecutorService.class).in(Scopes.SINGLETON);
        expose(DatasetOpExecutorService.class);

        bind(DatasetOpExecutor.class).to(YarnDatasetOpExecutor.class);
        expose(DatasetOpExecutor.class);
      }
    };
  }
}
