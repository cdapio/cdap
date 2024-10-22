/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.data.runtime;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.runtime.RuntimeModule;
import io.cdap.cdap.data2.datafabric.dataset.service.AuthorizationDatasetTypeService;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetTypeService;
import io.cdap.cdap.data2.datafabric.dataset.service.DefaultDatasetTypeService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetAdminOpHTTPHandler;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.RemoteDatasetOpExecutor;
import io.cdap.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.DefaultDatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.dataset2.StaticDatasetFramework;
import io.cdap.cdap.data2.metrics.DatasetMetricsReporter;
import io.cdap.cdap.data2.metrics.LevelDBDatasetMetricsReporter;
import io.cdap.cdap.gateway.handlers.CommonHandlers;
import io.cdap.http.HttpHandler;
import java.util.Map;

/**
 * Bindings for DataSet Service.
 */
public class DataSetServiceModules extends RuntimeModule {

  public static final String NOAUTH_DATASET_TYPE_SERVICE = "noAuthDatasetTypeService";

  @Override
  public Module getInMemoryModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        // Add the system dataset runtime module as public binding so that adding bindings could be added
        install(new SystemDatasetRuntimeModule().getInMemoryModules());
        install(new PrivateModule() {
          @Override
          protected void configure() {
            bind(DatasetDefinitionRegistryFactory.class)
                .to(DefaultDatasetDefinitionRegistryFactory.class).in(Scopes.SINGLETON);

            bind(DatasetFramework.class)
                .annotatedWith(Names.named("datasetMDS"))
                .toProvider(DatasetMdsProvider.class)
                .in(Singleton.class);
            expose(DatasetFramework.class).annotatedWith(Names.named("datasetMDS"));
            bind(DatasetService.class);
            expose(DatasetService.class);

            Named datasetUserName = Names.named(Constants.Service.DATASET_EXECUTOR);
            Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(),
                HttpHandler.class, datasetUserName);
            CommonHandlers.add(handlerBinder);
            handlerBinder.addBinding().to(DatasetAdminOpHTTPHandler.class);

            Multibinder.newSetBinder(binder(), DatasetMetricsReporter.class);

            bind(DatasetOpExecutorService.class).in(Scopes.SINGLETON);
            expose(DatasetOpExecutorService.class);

            bind(DatasetOpExecutor.class).to(RemoteDatasetOpExecutor.class);
            expose(DatasetOpExecutor.class);

            bind(DatasetTypeService.class)
                .annotatedWith(Names.named(NOAUTH_DATASET_TYPE_SERVICE))
                .to(DefaultDatasetTypeService.class);

            bind(DatasetTypeService.class).to(AuthorizationDatasetTypeService.class);
            expose(DatasetTypeService.class);
          }
        });
      }
    };
  }

  @Override
  public Module getStandaloneModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        // Add the system dataset runtime module as public binding so that adding bindings could be added
        install(new SystemDatasetRuntimeModule().getStandaloneModules());
        install(new PrivateModule() {
          @Override
          protected void configure() {
            bind(DatasetDefinitionRegistryFactory.class)
                .to(DefaultDatasetDefinitionRegistryFactory.class).in(Scopes.SINGLETON);

            bind(DatasetFramework.class)
                .annotatedWith(Names.named("datasetMDS"))
                .toProvider(DatasetMdsProvider.class)
                .in(Singleton.class);
            expose(DatasetFramework.class).annotatedWith(Names.named("datasetMDS"));

            Multibinder.newSetBinder(binder(), DatasetMetricsReporter.class)
                .addBinding().to(LevelDBDatasetMetricsReporter.class);

            bind(DatasetService.class);
            expose(DatasetService.class);

            Named datasetUserName = Names.named(Constants.Service.DATASET_EXECUTOR);
            Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(),
                HttpHandler.class, datasetUserName);
            CommonHandlers.add(handlerBinder);
            handlerBinder.addBinding().to(DatasetAdminOpHTTPHandler.class);

            bind(DatasetOpExecutorService.class).in(Scopes.SINGLETON);
            expose(DatasetOpExecutorService.class);

            bind(DatasetOpExecutor.class).to(RemoteDatasetOpExecutor.class);
            expose(DatasetOpExecutor.class);

            bind(DatasetTypeService.class)
                .annotatedWith(Names.named(NOAUTH_DATASET_TYPE_SERVICE))
                .to(DefaultDatasetTypeService.class);

            bind(DatasetTypeService.class).to(AuthorizationDatasetTypeService.class);
            expose(DatasetTypeService.class);
          }
        });
      }
    };
  }

  @Override
  public Module getDistributedModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        // Add the system dataset runtime module as public binding so that adding bindings could be added
        install(new SystemDatasetRuntimeModule().getDistributedModules());
        install(new PrivateModule() {
          @Override
          protected void configure() {
            bind(DatasetDefinitionRegistryFactory.class)
                .to(DefaultDatasetDefinitionRegistryFactory.class).in(Scopes.SINGLETON);

            bind(DatasetFramework.class)
                .annotatedWith(Names.named("datasetMDS"))
                .toProvider(DatasetMdsProvider.class)
                .in(Singleton.class);
            expose(DatasetFramework.class).annotatedWith(Names.named("datasetMDS"));

            Multibinder.newSetBinder(binder(), DatasetMetricsReporter.class)
                .addBinding().to(LevelDBDatasetMetricsReporter.class);

            // NOTE: this cannot be a singleton, because MasterServiceMain needs to obtain a new instance
            //       every time it becomes leader and starts a dataset service.
            bind(DatasetService.class);
            expose(DatasetService.class);

            Named datasetUserName = Names.named(Constants.Service.DATASET_EXECUTOR);
            Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(),
                HttpHandler.class, datasetUserName);
            CommonHandlers.add(handlerBinder);
            handlerBinder.addBinding().to(DatasetAdminOpHTTPHandler.class);

            bind(DatasetOpExecutorService.class).in(Scopes.SINGLETON);
            expose(DatasetOpExecutorService.class);

            bind(DatasetOpExecutor.class).to(RemoteDatasetOpExecutor.class);
            expose(DatasetOpExecutor.class);

            bind(DatasetTypeService.class)
                .annotatedWith(Names.named(NOAUTH_DATASET_TYPE_SERVICE))
                .to(DefaultDatasetTypeService.class);

            bind(DatasetTypeService.class).to(AuthorizationDatasetTypeService.class);
            expose(DatasetTypeService.class);

          }
        });
      }
    };
  }

  private static final class DatasetMdsProvider implements Provider<DatasetFramework> {

    private final DatasetDefinitionRegistryFactory registryFactory;
    private final Map<String, DatasetModule> defaultModules;

    @Inject
    DatasetMdsProvider(DatasetDefinitionRegistryFactory registryFactory,
        @Constants.Dataset.Manager.DefaultDatasetModules Map<String, DatasetModule> defaultModules) {
      this.registryFactory = registryFactory;
      this.defaultModules = defaultModules;
    }

    @Override
    public DatasetFramework get() {
      Map<String, DatasetModule> modulesMap = ImmutableMap.<String, DatasetModule>builder()
          .putAll(defaultModules)
          .build();
      // NOTE: it is fine to use in-memory dataset manager for direct access to dataset MDS even in distributed mode
      //       as long as the data is durably persisted
      return new StaticDatasetFramework(registryFactory, modulesMap);
    }
  }
}
