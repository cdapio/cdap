package com.continuuity.data.runtime;

import com.continuuity.common.conf.Constants;
import com.continuuity.data2.datafabric.dataset.DataFabricDatasetManager;
import com.continuuity.data2.datafabric.dataset.service.DatasetManagerService;
import com.continuuity.data2.dataset2.manager.DatasetManager;
import com.continuuity.data2.dataset2.manager.inmemory.DefaultDatasetDefinitionRegistry;
import com.continuuity.data2.dataset2.manager.inmemory.InMemoryDatasetManager;
import com.continuuity.data2.dataset2.module.lib.TableModule;
import com.continuuity.data2.dataset2.module.lib.hbase.HBaseTableModule;
import com.continuuity.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import com.continuuity.data2.dataset2.module.lib.leveldb.LevelDBTableModule;
import com.continuuity.data2.dataset2.user.DatasetAdminHTTPHandler;
import com.continuuity.data2.dataset2.user.DatasetUserService;
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

        // TODO: remove once DatasetUserService is run on-demand
        Named datasetUserName = Names.named(Constants.Service.DATASET_USER);
        Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class, datasetUserName);
        handlerBinder.addBinding().to(DatasetAdminHTTPHandler.class);
        handlerBinder.addBinding().to(PingHandler.class);
        bind(DatasetUserService.class).in(Scopes.SINGLETON);
        expose(DatasetUserService.class);
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

        // TODO: remove once DatasetUserService is run on-demand
        Named datasetUserName = Names.named(Constants.Service.DATASET_USER);
        Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class, datasetUserName);
        handlerBinder.addBinding().to(DatasetAdminHTTPHandler.class);
        handlerBinder.addBinding().to(PingHandler.class);
        bind(DatasetUserService.class).in(Scopes.SINGLETON);
        expose(DatasetUserService.class);
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

        // TODO: remove once DatasetUserService is run on-demand
        Named datasetUserName = Names.named(Constants.Service.DATASET_USER);
        Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class, datasetUserName);
        handlerBinder.addBinding().to(DatasetAdminHTTPHandler.class);
        handlerBinder.addBinding().to(PingHandler.class);
        bind(DatasetUserService.class).in(Scopes.SINGLETON);
        expose(DatasetUserService.class);
      }
    };
  }
}
