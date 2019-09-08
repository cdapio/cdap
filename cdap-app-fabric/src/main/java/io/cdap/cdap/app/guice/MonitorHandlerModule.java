/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.app.guice;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.app.store.ServiceStore;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.twill.MasterServiceManager;
import io.cdap.cdap.data2.datafabric.dataset.DatasetExecutorServiceManager;
import io.cdap.cdap.data2.datafabric.dataset.MetadataServiceManager;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.DefaultDatasetDefinitionRegistryFactory;
import io.cdap.cdap.data2.dataset2.InMemoryDatasetFramework;
import io.cdap.cdap.data2.dataset2.lib.kv.HBaseKVTableDefinition;
import io.cdap.cdap.data2.dataset2.lib.kv.InMemoryKVTableDefinition;
import io.cdap.cdap.explore.service.ExploreServiceManager;
import io.cdap.cdap.gateway.handlers.DatasetServiceStore;
import io.cdap.cdap.gateway.handlers.MonitorHandler;
import io.cdap.cdap.internal.app.runtime.distributed.AppFabricServiceManager;
import io.cdap.cdap.internal.app.runtime.distributed.TransactionServiceManager;
import io.cdap.cdap.internal.app.runtime.monitor.NonHadoopAppFabricServiceManager;
import io.cdap.cdap.internal.app.services.AppFabricServer;
import io.cdap.cdap.logging.run.LogSaverServiceManager;
import io.cdap.cdap.messaging.distributed.MessagingServiceManager;
import io.cdap.cdap.metrics.runtime.MetricsProcessorStatusServiceManager;
import io.cdap.cdap.metrics.runtime.MetricsServiceManager;
import io.cdap.http.HttpHandler;

import java.util.Map;

/**
 * This Guice module providing binding of the {@link MonitorHandler} that get used inside {@link AppFabricServer}.
 */
public class MonitorHandlerModule extends AbstractModule {

  private static final String SERVICE_STORE_DS_MODULES = "service.store.ds.modules";
  private final boolean isHadoop;

  public MonitorHandlerModule(boolean isHadoop) {
    this.isHadoop = isHadoop;
  }

  @Override
  protected void configure() {
    // Only MonitorHandler use MasterServiceManager
    install(new PrivateModule() {
      @Override
      protected void configure() {
        if (isHadoop) {
          addHadoopBindings(binder());
        } else {
          addNonHadoopBindings(binder());
        }

        bind(DatasetFramework.class)
          .annotatedWith(Names.named("local.ds.framework")).toProvider(DatasetFrameworkProvider.class);

        // Need to expose ServiceStore for master and standalone main to start/stop it
        bind(ServiceStore.class).to(DatasetServiceStore.class).in(Scopes.SINGLETON);
        expose(ServiceStore.class);

        // Expose the MonitorHandler so that it can be bounded to the multibinder
        bind(MonitorHandler.class);
        expose(MonitorHandler.class);
      }
    });

    Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(
      binder(), HttpHandler.class, Names.named(Constants.AppFabric.HANDLERS_BINDING));

    handlerBinder.addBinding().to(MonitorHandler.class);
  }

  /**
   * Adds bindings for the {@link MonitorHandler} that are used in non-Hadoop environment.
   *
   * @param binder the guice {@link Binder} to add the bindings to
   */
  private void addNonHadoopBindings(Binder binder) {
    MapBinder<String, MasterServiceManager> mapBinder = MapBinder.newMapBinder(binder, String.class,
                                                                               MasterServiceManager.class);
    mapBinder.addBinding(Constants.Service.LOGSAVER)
      .toProvider(new NonHadoopMasterServiceManagerProvider(LogSaverServiceManager.class));
    mapBinder.addBinding(Constants.Service.TRANSACTION)
      .toProvider(new NonHadoopMasterServiceManagerProvider(TransactionServiceManager.class));
    mapBinder.addBinding(Constants.Service.METRICS_PROCESSOR)
      .toProvider(new NonHadoopMasterServiceManagerProvider(MetricsProcessorStatusServiceManager.class));
    mapBinder.addBinding(Constants.Service.METRICS)
      .toProvider(new NonHadoopMasterServiceManagerProvider(MetricsServiceManager.class));
    mapBinder.addBinding(Constants.Service.APP_FABRIC_HTTP)
      .toProvider(new NonHadoopMasterServiceManagerProvider(NonHadoopAppFabricServiceManager.class));
    mapBinder.addBinding(Constants.Service.DATASET_EXECUTOR)
      .toProvider(new NonHadoopMasterServiceManagerProvider(DatasetExecutorServiceManager.class));
    mapBinder.addBinding(Constants.Service.METADATA_SERVICE)
      .toProvider(new NonHadoopMasterServiceManagerProvider(MetadataServiceManager.class));
    mapBinder.addBinding(Constants.Service.EXPLORE_HTTP_USER_SERVICE)
      .toProvider(new NonHadoopMasterServiceManagerProvider(ExploreServiceManager.class));
    mapBinder.addBinding(Constants.Service.MESSAGING_SERVICE)
      .toProvider(new NonHadoopMasterServiceManagerProvider(MessagingServiceManager.class));

    // The ServiceStore uses a special non-TX KV Table.
    bindDatasetModule(binder, new InMemoryKVTableDefinition.Module());
  }

  /**
   * Adds bindings for the {@link MonitorHandler} that are used in Hadoop environment.
   *
   * @param binder the guice {@link Binder} to add the bindings to
   */
  private void addHadoopBindings(Binder binder) {
    MapBinder<String, MasterServiceManager> mapBinder = MapBinder.newMapBinder(binder, String.class,
                                                                               MasterServiceManager.class);
    mapBinder.addBinding(Constants.Service.LOGSAVER).to(LogSaverServiceManager.class);
    mapBinder.addBinding(Constants.Service.TRANSACTION).to(TransactionServiceManager.class);
    mapBinder.addBinding(Constants.Service.METRICS_PROCESSOR).to(MetricsProcessorStatusServiceManager.class);
    mapBinder.addBinding(Constants.Service.METRICS).to(MetricsServiceManager.class);
    mapBinder.addBinding(Constants.Service.APP_FABRIC_HTTP).to(AppFabricServiceManager.class);
    mapBinder.addBinding(Constants.Service.DATASET_EXECUTOR).to(DatasetExecutorServiceManager.class);
    mapBinder.addBinding(Constants.Service.METADATA_SERVICE).to(MetadataServiceManager.class);
    mapBinder.addBinding(Constants.Service.EXPLORE_HTTP_USER_SERVICE).to(ExploreServiceManager.class);
    mapBinder.addBinding(Constants.Service.MESSAGING_SERVICE).to(MessagingServiceManager.class);

    // The ServiceStore uses a special non-TX KV Table.
    bindDatasetModule(binder, new HBaseKVTableDefinition.Module());
  }

  /**
   * Adds special {@link DatasetModule} bindings for the {@link ServiceStore}.
   */
  private void bindDatasetModule(Binder binder, DatasetModule module) {
    MapBinder<String, DatasetModule> mapBinder = MapBinder.newMapBinder(
      binder, String.class, DatasetModule.class, Names.named(SERVICE_STORE_DS_MODULES));

    mapBinder.addBinding(module.getClass().getName()).toInstance(module);
  }

  /**
   * A Guice provider for {@link DatasetFramework} with {@link InMemoryDatasetFramework} as implementation.
   */
  private static final class DatasetFrameworkProvider implements Provider<DatasetFramework> {

    private final Injector injector;
    private final Map<String, DatasetModule> datasetModules;

    @Inject
    private DatasetFrameworkProvider(Injector injector,
                                     @Named(SERVICE_STORE_DS_MODULES) Map<String, DatasetModule> datasetModules) {
      this.injector = injector;
      this.datasetModules = datasetModules;
    }

    @Override
    public DatasetFramework get() {
      return new InMemoryDatasetFramework(new DefaultDatasetDefinitionRegistryFactory(injector), datasetModules);
    }
  }

  /**
   * Provides for {@link MasterServiceManager} used in non-hadoop environment.
   */
  private static final class NonHadoopMasterServiceManagerProvider implements Provider<MasterServiceManager> {

    private final Class<? extends MasterServiceManager> serviceManagerClass;
    @Inject
    private Injector injector;

    NonHadoopMasterServiceManagerProvider(Class<? extends MasterServiceManager> serviceManagerClass) {
      this.serviceManagerClass = serviceManagerClass;
    }

    @Override
    public MasterServiceManager get() {
      return new DelegatingMasterServiceManager(injector.getInstance(serviceManagerClass)) {
        @Override
        public int getInstances() {
          // For now it is always just one instance for each master service in non-hadoop environment.
          return 1;
        }
      };
    }
  }
}
