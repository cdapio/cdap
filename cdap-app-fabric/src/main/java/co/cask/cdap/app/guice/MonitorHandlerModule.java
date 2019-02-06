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

package co.cask.cdap.app.guice;

import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.app.store.ServiceStore;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.twill.MasterServiceManager;
import co.cask.cdap.data2.datafabric.dataset.DatasetExecutorServiceManager;
import co.cask.cdap.data2.datafabric.dataset.MetadataServiceManager;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.lib.kv.HBaseKVTableDefinition;
import co.cask.cdap.data2.dataset2.lib.kv.InMemoryKVTableDefinition;
import co.cask.cdap.explore.service.ExploreServiceManager;
import co.cask.cdap.gateway.handlers.DatasetServiceStore;
import co.cask.cdap.gateway.handlers.MonitorHandler;
import co.cask.cdap.internal.app.runtime.batch.InMemoryTransactionServiceManager;
import co.cask.cdap.internal.app.runtime.distributed.AppFabricServiceManager;
import co.cask.cdap.internal.app.runtime.distributed.TransactionServiceManager;
import co.cask.cdap.internal.app.services.AppFabricServer;
import co.cask.cdap.logging.run.InMemoryAppFabricServiceManager;
import co.cask.cdap.logging.run.InMemoryDatasetExecutorServiceManager;
import co.cask.cdap.logging.run.InMemoryExploreServiceManager;
import co.cask.cdap.logging.run.InMemoryLogSaverServiceManager;
import co.cask.cdap.logging.run.InMemoryMessagingServiceManager;
import co.cask.cdap.logging.run.InMemoryMetadataServiceManager;
import co.cask.cdap.logging.run.InMemoryMetricsProcessorServiceManager;
import co.cask.cdap.logging.run.InMemoryMetricsServiceManager;
import co.cask.cdap.logging.run.LogSaverStatusServiceManager;
import co.cask.cdap.messaging.distributed.MessagingServiceManager;
import co.cask.cdap.metrics.runtime.MetricsProcessorStatusServiceManager;
import co.cask.cdap.metrics.runtime.MetricsServiceManager;
import co.cask.http.HttpHandler;
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

import java.util.Map;

/**
 * This Guice module providing binding of the {@link MonitorHandler} that get used inside {@link AppFabricServer}.
 */
public class MonitorHandlerModule extends AbstractModule {

  public static final String SERVICE_STORE_DS_MODULES = "service.store.ds.modules";
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
    mapBinder.addBinding(Constants.Service.LOGSAVER).to(InMemoryLogSaverServiceManager.class);
    mapBinder.addBinding(Constants.Service.TRANSACTION).to(InMemoryTransactionServiceManager.class);
    mapBinder.addBinding(Constants.Service.METRICS_PROCESSOR).to(InMemoryMetricsProcessorServiceManager.class);
    mapBinder.addBinding(Constants.Service.METRICS).to(InMemoryMetricsServiceManager.class);
    mapBinder.addBinding(Constants.Service.APP_FABRIC_HTTP).to(InMemoryAppFabricServiceManager.class);
    mapBinder.addBinding(Constants.Service.DATASET_EXECUTOR).to(InMemoryDatasetExecutorServiceManager.class);
    mapBinder.addBinding(Constants.Service.METADATA_SERVICE).to(InMemoryMetadataServiceManager.class);
    mapBinder.addBinding(Constants.Service.EXPLORE_HTTP_USER_SERVICE).to(InMemoryExploreServiceManager.class);
    mapBinder.addBinding(Constants.Service.MESSAGING_SERVICE).to(InMemoryMessagingServiceManager.class);

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
    mapBinder.addBinding(Constants.Service.LOGSAVER).to(LogSaverStatusServiceManager.class);
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
      binder(), String.class, DatasetModule.class, Names.named(SERVICE_STORE_DS_MODULES));

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
}
