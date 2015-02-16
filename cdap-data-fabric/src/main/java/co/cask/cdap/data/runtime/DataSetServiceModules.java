/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data.runtime;

import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetAdminOpHTTPHandler;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.LocalDatasetOpExecutor;
import co.cask.cdap.data2.datafabric.dataset.service.executor.YarnDatasetOpExecutor;
import co.cask.cdap.data2.datafabric.dataset.service.mds.MDSDatasetsRegistry;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.lib.file.FileSetModule;
import co.cask.cdap.data2.dataset2.lib.partitioned.TimePartitionedFileSetModule;
import co.cask.cdap.data2.dataset2.lib.table.ACLStoreTableModule;
import co.cask.cdap.data2.dataset2.lib.table.CoreDatasetsModule;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseMetricsTableModule;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseOrderedTableModule;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryMetricsTableModule;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryOrderedTableModule;
import co.cask.cdap.data2.dataset2.module.lib.leveldb.LevelDBMetricsTableModule;
import co.cask.cdap.data2.dataset2.module.lib.leveldb.LevelDBOrderedTableModule;
import co.cask.cdap.data2.metrics.DatasetMetricsReporter;
import co.cask.cdap.data2.metrics.HBaseDatasetMetricsReporter;
import co.cask.cdap.data2.metrics.LevelDBDatasetMetricsReporter;
import co.cask.cdap.gateway.handlers.CommonHandlers;
import co.cask.http.HttpHandler;
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
  public static final Map<String, DatasetModule> INMEMORY_DATASET_MODULES;

  static {
    INMEMORY_DATASET_MODULES = Maps.newLinkedHashMap();
    // NOTE: order is important due to dependencies between modules
    INMEMORY_DATASET_MODULES.put("orderedTable-memory", new InMemoryOrderedTableModule());
    INMEMORY_DATASET_MODULES.put("metricsTable-memory", new InMemoryMetricsTableModule());
    INMEMORY_DATASET_MODULES.put("core", new CoreDatasetsModule());
    INMEMORY_DATASET_MODULES.put("fileSet", new FileSetModule());
    INMEMORY_DATASET_MODULES.put("timePartitionedFileSet", new TimePartitionedFileSetModule());
    INMEMORY_DATASET_MODULES.put("aclStoreTable", new ACLStoreTableModule());
  }

  public Module getInMemoryModule() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        // NOTE: order is important due to dependencies between modules
        Map<String, DatasetModule> defaultModules = Maps.newLinkedHashMap();
        defaultModules.put("orderedTable-memory", new InMemoryOrderedTableModule());
        defaultModules.put("metricsTable-memory", new InMemoryMetricsTableModule());
        defaultModules.put("core", new CoreDatasetsModule());
        defaultModules.put("aclStoreTable", new ACLStoreTableModule());
        defaultModules.put("fileSet", new FileSetModule());
        defaultModules.put("timePartitionedFileSet", new TimePartitionedFileSetModule());

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
        CommonHandlers.add(handlerBinder);
        handlerBinder.addBinding().to(DatasetAdminOpHTTPHandler.class);

        Multibinder.newSetBinder(binder(), DatasetMetricsReporter.class);

        bind(DatasetOpExecutorService.class).in(Scopes.SINGLETON);
        expose(DatasetOpExecutorService.class);

        bind(DatasetOpExecutor.class).to(LocalDatasetOpExecutor.class);
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
        defaultModules.put("metricsTable-leveldb", new LevelDBMetricsTableModule());
        defaultModules.put("core", new CoreDatasetsModule());
        defaultModules.put("fileSet", new FileSetModule());
        defaultModules.put("timePartitionedFileSet", new TimePartitionedFileSetModule());
        defaultModules.put("aclStoreTable", new ACLStoreTableModule());

        bind(new TypeLiteral<Map<String, ? extends DatasetModule>>() { })
          .annotatedWith(Names.named("defaultDatasetModules")).toInstance(defaultModules);

        install(new FactoryModuleBuilder()
                  .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                  .build(DatasetDefinitionRegistryFactory.class));
        // NOTE: it is fine to use in-memory dataset manager for direct access to dataset MDS even in distributed mode
        //       as long as the data is durably persisted
        bind(DatasetFramework.class).annotatedWith(Names.named("datasetMDS")).to(InMemoryDatasetFramework.class);
        bind(MDSDatasetsRegistry.class).in(Singleton.class);

        Multibinder.newSetBinder(binder(), DatasetMetricsReporter.class)
          .addBinding().to(LevelDBDatasetMetricsReporter.class);

        bind(DatasetService.class);
        expose(DatasetService.class);

        Named datasetUserName = Names.named(Constants.Service.DATASET_EXECUTOR);
        Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class, datasetUserName);
        CommonHandlers.add(handlerBinder);
        handlerBinder.addBinding().to(DatasetAdminOpHTTPHandler.class);

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
        defaultModules.put("metricsTable-hbase", new HBaseMetricsTableModule());
        defaultModules.put("core", new CoreDatasetsModule());
        defaultModules.put("fileSet", new FileSetModule());
        defaultModules.put("timePartitionedFileSet", new TimePartitionedFileSetModule());
        defaultModules.put("aclStoreTable", new ACLStoreTableModule());

        bind(new TypeLiteral<Map<String, ? extends DatasetModule>>() { })
          .annotatedWith(Names.named("defaultDatasetModules")).toInstance(defaultModules);

        install(new FactoryModuleBuilder()
                  .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                  .build(DatasetDefinitionRegistryFactory.class));
        // NOTE: it is fine to use in-memory dataset manager for direct access to dataset MDS even in distributed mode
        //       as long as the data is durably persisted
        bind(DatasetFramework.class).annotatedWith(Names.named("datasetMDS")).to(InMemoryDatasetFramework.class);
        bind(MDSDatasetsRegistry.class).in(Singleton.class);

        Multibinder.newSetBinder(binder(), DatasetMetricsReporter.class)
          .addBinding().to(HBaseDatasetMetricsReporter.class);

        // NOTE: this cannot be a singleton, because MasterServiceMain needs to obtain a new instance
        //       every time it becomes leader and starts a dataset service.
        bind(DatasetService.class);
        expose(DatasetService.class);

        Named datasetUserName = Names.named(Constants.Service.DATASET_EXECUTOR);
        Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class, datasetUserName);
        CommonHandlers.add(handlerBinder);
        handlerBinder.addBinding().to(DatasetAdminOpHTTPHandler.class);

        bind(DatasetOpExecutorService.class).in(Scopes.SINGLETON);
        expose(DatasetOpExecutorService.class);

        bind(DatasetOpExecutor.class).to(YarnDatasetOpExecutor.class);
        expose(DatasetOpExecutor.class);
      }
    };
  }
}
