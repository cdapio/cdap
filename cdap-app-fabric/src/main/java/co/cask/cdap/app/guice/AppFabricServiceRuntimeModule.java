/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.app.deploy.Manager;
import co.cask.cdap.app.deploy.ManagerFactory;
import co.cask.cdap.app.mapreduce.DistributedMRJobInfoFetcher;
import co.cask.cdap.app.mapreduce.LocalMRJobInfoFetcher;
import co.cask.cdap.app.mapreduce.MRJobInfoFetcher;
import co.cask.cdap.app.store.RuntimeStore;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.common.security.DefaultUGIProvider;
import co.cask.cdap.common.security.UGIProvider;
import co.cask.cdap.common.security.UnsupportedUGIProvider;
import co.cask.cdap.common.twill.MasterServiceManager;
import co.cask.cdap.common.utils.Networks;
import co.cask.cdap.config.guice.ConfigStoreModule;
import co.cask.cdap.data.stream.StreamServiceManager;
import co.cask.cdap.data.stream.StreamViewHttpHandler;
import co.cask.cdap.data.stream.service.StreamFetchHandler;
import co.cask.cdap.data.stream.service.StreamHandler;
import co.cask.cdap.data2.datafabric.dataset.DatasetExecutorServiceManager;
import co.cask.cdap.data2.datafabric.dataset.MetadataServiceManager;
import co.cask.cdap.data2.datafabric.dataset.RemoteSystemOperationServiceManager;
import co.cask.cdap.explore.service.ExploreServiceManager;
import co.cask.cdap.gateway.handlers.AppLifecycleHttpHandler;
import co.cask.cdap.gateway.handlers.ArtifactHttpHandler;
import co.cask.cdap.gateway.handlers.AuthorizationHandler;
import co.cask.cdap.gateway.handlers.CommonHandlers;
import co.cask.cdap.gateway.handlers.ConfigHandler;
import co.cask.cdap.gateway.handlers.ConsoleSettingsHttpHandler;
import co.cask.cdap.gateway.handlers.DashboardHttpHandler;
import co.cask.cdap.gateway.handlers.ImpersonationHandler;
import co.cask.cdap.gateway.handlers.MonitorHandler;
import co.cask.cdap.gateway.handlers.NamespaceHttpHandler;
import co.cask.cdap.gateway.handlers.NotificationFeedHttpHandler;
import co.cask.cdap.gateway.handlers.OperationalStatsHttpHandler;
import co.cask.cdap.gateway.handlers.PreferencesHttpHandler;
import co.cask.cdap.gateway.handlers.ProgramLifecycleHttpHandler;
import co.cask.cdap.gateway.handlers.RouteConfigHttpHandler;
import co.cask.cdap.gateway.handlers.SecureStoreHandler;
import co.cask.cdap.gateway.handlers.TransactionHttpHandler;
import co.cask.cdap.gateway.handlers.UsageHandler;
import co.cask.cdap.gateway.handlers.VersionHandler;
import co.cask.cdap.gateway.handlers.WorkflowHttpHandler;
import co.cask.cdap.gateway.handlers.WorkflowStatsSLAHttpHandler;
import co.cask.cdap.gateway.handlers.meta.RemotePrivilegesHandler;
import co.cask.cdap.gateway.handlers.preview.PreviewHttpHandler;
import co.cask.cdap.internal.app.deploy.LocalApplicationManager;
import co.cask.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.namespace.DefaultNamespaceAdmin;
import co.cask.cdap.internal.app.namespace.DefaultNamespaceResourceDeleter;
import co.cask.cdap.internal.app.namespace.DistributedStorageProviderNamespaceAdmin;
import co.cask.cdap.internal.app.namespace.LocalStorageProviderNamespaceAdmin;
import co.cask.cdap.internal.app.namespace.NamespaceResourceDeleter;
import co.cask.cdap.internal.app.namespace.StorageProviderNamespaceAdmin;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactStore;
import co.cask.cdap.internal.app.runtime.batch.InMemoryTransactionServiceManager;
import co.cask.cdap.internal.app.runtime.distributed.AppFabricServiceManager;
import co.cask.cdap.internal.app.runtime.distributed.TransactionServiceManager;
import co.cask.cdap.internal.app.runtime.schedule.DistributedSchedulerService;
import co.cask.cdap.internal.app.runtime.schedule.ExecutorThreadPool;
import co.cask.cdap.internal.app.runtime.schedule.LocalSchedulerService;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerService;
import co.cask.cdap.internal.app.runtime.schedule.store.DatasetBasedTimeScheduleStore;
import co.cask.cdap.internal.app.services.AppFabricServer;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.internal.app.services.StandaloneAppFabricServer;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.internal.pipeline.SynchronousPipelineFactory;
import co.cask.cdap.logging.run.InMemoryAppFabricServiceManager;
import co.cask.cdap.logging.run.InMemoryDatasetExecutorServiceManager;
import co.cask.cdap.logging.run.InMemoryExploreServiceManager;
import co.cask.cdap.logging.run.InMemoryLogSaverServiceManager;
import co.cask.cdap.logging.run.InMemoryMetadataServiceManager;
import co.cask.cdap.logging.run.InMemoryMetricsProcessorServiceManager;
import co.cask.cdap.logging.run.InMemoryMetricsServiceManager;
import co.cask.cdap.logging.run.InMemoryStreamServiceManager;
import co.cask.cdap.logging.run.LogSaverStatusServiceManager;
import co.cask.cdap.metrics.runtime.MetricsProcessorStatusServiceManager;
import co.cask.cdap.metrics.runtime.MetricsServiceManager;
import co.cask.cdap.pipeline.PipelineFactory;
import co.cask.cdap.route.store.LocalRouteStore;
import co.cask.cdap.route.store.RouteStore;
import co.cask.cdap.route.store.ZKRouteStore;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.http.HttpHandler;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.quartz.SchedulerException;
import org.quartz.core.JobRunShellFactory;
import org.quartz.core.QuartzScheduler;
import org.quartz.core.QuartzSchedulerResources;
import org.quartz.impl.DefaultThreadExecutor;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.impl.StdJobRunShellFactory;
import org.quartz.impl.StdScheduler;
import org.quartz.simpl.CascadingClassLoadHelper;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * AppFabric Service Runtime Module.
 */
public final class AppFabricServiceRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return Modules.combine(new AppFabricServiceModule(
                             StreamHandler.class, StreamFetchHandler.class,
                             StreamViewHttpHandler.class),
                           new ConfigStoreModule().getInMemoryModule(),
                           new EntityVerifierModule(),
                           new AuthenticationContextModules().getMasterModule(),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               bind(SchedulerService.class).to(LocalSchedulerService.class).in(Scopes.SINGLETON);
                               bind(Scheduler.class).to(SchedulerService.class);
                               bind(MRJobInfoFetcher.class).to(LocalMRJobInfoFetcher.class);
                               bind(StorageProviderNamespaceAdmin.class).to(LocalStorageProviderNamespaceAdmin.class);
                               bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
                               bind(RouteStore.class).to(LocalRouteStore.class).in(Scopes.SINGLETON);
                               addInMemoryBindings(binder());

                               Multibinder<String> servicesNamesBinder =
                                 Multibinder.newSetBinder(binder(), String.class,
                                                          Names.named("appfabric.services.names"));
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.APP_FABRIC_HTTP);
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.STREAMS);
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.MESSAGING_SERVICE);

                               Multibinder<String> handlerHookNamesBinder =
                                 Multibinder.newSetBinder(binder(), String.class,
                                                          Names.named("appfabric.handler.hooks"));
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Service.APP_FABRIC_HTTP);
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Stream.STREAM_HANDLER);
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Service.MESSAGING_SERVICE);
                             }
                           });
  }

  @Override
  public Module getStandaloneModules() {

    return Modules.combine(new AppFabricServiceModule(
                             StreamHandler.class, StreamFetchHandler.class,
                             StreamViewHttpHandler.class, PreviewHttpHandler.class),
                           new ConfigStoreModule().getStandaloneModule(),
                           new EntityVerifierModule(),
                           new AuthenticationContextModules().getMasterModule(),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               bind(AppFabricServer.class).to(StandaloneAppFabricServer.class).in(Scopes.SINGLETON);
                               bind(SchedulerService.class).to(LocalSchedulerService.class).in(Scopes.SINGLETON);
                               bind(Scheduler.class).to(SchedulerService.class);
                               bind(MRJobInfoFetcher.class).to(LocalMRJobInfoFetcher.class);
                               bind(StorageProviderNamespaceAdmin.class).to(LocalStorageProviderNamespaceAdmin.class);
                               bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
                               bind(RouteStore.class).to(LocalRouteStore.class).in(Scopes.SINGLETON);
                               addInMemoryBindings(binder());

                               Multibinder<String> servicesNamesBinder =
                                 Multibinder.newSetBinder(binder(), String.class,
                                                          Names.named("appfabric.services.names"));
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.APP_FABRIC_HTTP);
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.STREAMS);
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.PREVIEW_HTTP);
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.MESSAGING_SERVICE);

                               Multibinder<String> handlerHookNamesBinder =
                                 Multibinder.newSetBinder(binder(), String.class,
                                                          Names.named("appfabric.handler.hooks"));
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Service.APP_FABRIC_HTTP);
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Stream.STREAM_HANDLER);
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Service.PREVIEW_HTTP);
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Service.MESSAGING_SERVICE);
                             }
                           });
  }

  private void addInMemoryBindings(Binder binder) {
    MapBinder<String, MasterServiceManager> mapBinder = MapBinder.newMapBinder(
      binder, String.class, MasterServiceManager.class);
    mapBinder.addBinding(Constants.Service.LOGSAVER)
      .to(InMemoryLogSaverServiceManager.class);
    mapBinder.addBinding(Constants.Service.TRANSACTION)
      .to(InMemoryTransactionServiceManager.class);
    mapBinder.addBinding(Constants.Service.METRICS_PROCESSOR)
      .to(InMemoryMetricsProcessorServiceManager.class);
    mapBinder.addBinding(Constants.Service.METRICS)
      .to(InMemoryMetricsServiceManager.class);
    mapBinder.addBinding(Constants.Service.APP_FABRIC_HTTP)
      .to(InMemoryAppFabricServiceManager.class);
    mapBinder.addBinding(Constants.Service.STREAMS)
      .to(InMemoryStreamServiceManager.class);
    mapBinder.addBinding(Constants.Service.DATASET_EXECUTOR)
      .to(InMemoryDatasetExecutorServiceManager.class);
    mapBinder.addBinding(Constants.Service.METADATA_SERVICE)
      .to(InMemoryMetadataServiceManager.class);
    mapBinder.addBinding(Constants.Service.EXPLORE_HTTP_USER_SERVICE)
      .to(InMemoryExploreServiceManager.class);
  }

  @Override
  public Module getDistributedModules() {

    return Modules.combine(new AppFabricServiceModule(ImpersonationHandler.class),
                           new ConfigStoreModule().getDistributedModule(),
                           new EntityVerifierModule(),
                           new AuthenticationContextModules().getMasterModule(),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               bind(SchedulerService.class).to(DistributedSchedulerService.class).in(Scopes.SINGLETON);
                               bind(Scheduler.class).to(SchedulerService.class);
                               bind(MRJobInfoFetcher.class).to(DistributedMRJobInfoFetcher.class);
                               bind(StorageProviderNamespaceAdmin.class)
                                 .to(DistributedStorageProviderNamespaceAdmin.class);
                               bind(UGIProvider.class).to(DefaultUGIProvider.class);
                               bind(RouteStore.class).to(ZKRouteStore.class).in(Scopes.SINGLETON);
                               MapBinder<String, MasterServiceManager> mapBinder = MapBinder.newMapBinder(
                                 binder(), String.class, MasterServiceManager.class);
                               mapBinder.addBinding(Constants.Service.LOGSAVER)
                                        .to(LogSaverStatusServiceManager.class);
                               mapBinder.addBinding(Constants.Service.TRANSACTION)
                                        .to(TransactionServiceManager.class);
                               mapBinder.addBinding(Constants.Service.METRICS_PROCESSOR)
                                        .to(MetricsProcessorStatusServiceManager.class);
                               mapBinder.addBinding(Constants.Service.METRICS)
                                        .to(MetricsServiceManager.class);
                               mapBinder.addBinding(Constants.Service.APP_FABRIC_HTTP)
                                        .to(AppFabricServiceManager.class);
                               mapBinder.addBinding(Constants.Service.STREAMS)
                                        .to(StreamServiceManager.class);
                               mapBinder.addBinding(Constants.Service.DATASET_EXECUTOR)
                                        .to(DatasetExecutorServiceManager.class);
                               mapBinder.addBinding(Constants.Service.METADATA_SERVICE)
                                        .to(MetadataServiceManager.class);
                               mapBinder.addBinding(Constants.Service.REMOTE_SYSTEM_OPERATION)
                                        .to(RemoteSystemOperationServiceManager.class);
                               mapBinder.addBinding(Constants.Service.EXPLORE_HTTP_USER_SERVICE)
                                        .to(ExploreServiceManager.class);

                               Multibinder<String> servicesNamesBinder =
                                 Multibinder.newSetBinder(binder(), String.class,
                                                          Names.named("appfabric.services.names"));
                               servicesNamesBinder.addBinding().toInstance(Constants.Service.APP_FABRIC_HTTP);

                               Multibinder<String> handlerHookNamesBinder =
                                 Multibinder.newSetBinder(binder(), String.class,
                                                          Names.named("appfabric.handler.hooks"));
                               handlerHookNamesBinder.addBinding().toInstance(Constants.Service.APP_FABRIC_HTTP);
                             }
                           });
  }

  /**
   * Guice module for AppFabricServer. Requires data-fabric related bindings being available.
   */
  private static final class AppFabricServiceModule extends AbstractModule {

    private final List<Class<? extends HttpHandler>> handlerClasses;

    @SafeVarargs
    private AppFabricServiceModule(Class<? extends HttpHandler>... handlerClasses) {
      this.handlerClasses = ImmutableList.copyOf(handlerClasses);
    }

    @Override
    protected void configure() {
      bind(PipelineFactory.class).to(SynchronousPipelineFactory.class);

      install(
        new FactoryModuleBuilder()
          .implement(new TypeLiteral<Manager<AppDeploymentInfo, ApplicationWithPrograms>>() {
                     },
                     new TypeLiteral<LocalApplicationManager<AppDeploymentInfo, ApplicationWithPrograms>>() {
                     })
          .build(new TypeLiteral<ManagerFactory<AppDeploymentInfo, ApplicationWithPrograms>>() {
          })
      );

      bind(Store.class).to(DefaultStore.class);
      // we can simply use DefaultStore for RuntimeStore, when its not running in a separate container
      bind(RuntimeStore.class).to(DefaultStore.class);
      bind(ArtifactStore.class).in(Scopes.SINGLETON);
      bind(ProgramLifecycleService.class).in(Scopes.SINGLETON);

      install(new PrivateModule() {
        @Override
        protected void configure() {
          bind(NamespaceResourceDeleter.class).to(DefaultNamespaceResourceDeleter.class).in(Scopes.SINGLETON);
          bind(DefaultNamespaceAdmin.class).in(Scopes.SINGLETON);
          bind(NamespaceAdmin.class).to(DefaultNamespaceAdmin.class);
          bind(NamespaceQueryAdmin.class).to(DefaultNamespaceAdmin.class);

          expose(NamespaceAdmin.class);
          expose(NamespaceQueryAdmin.class);
        }
      });

      Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(
        binder(), HttpHandler.class, Names.named(Constants.AppFabric.HANDLERS_BINDING));

      CommonHandlers.add(handlerBinder);
      handlerBinder.addBinding().to(ConfigHandler.class);
      handlerBinder.addBinding().to(VersionHandler.class);
      handlerBinder.addBinding().to(MonitorHandler.class);
      handlerBinder.addBinding().to(UsageHandler.class);
      handlerBinder.addBinding().to(NamespaceHttpHandler.class);
      handlerBinder.addBinding().to(NotificationFeedHttpHandler.class);
      handlerBinder.addBinding().to(AppLifecycleHttpHandler.class);
      handlerBinder.addBinding().to(DashboardHttpHandler.class);
      handlerBinder.addBinding().to(ProgramLifecycleHttpHandler.class);
      handlerBinder.addBinding().to(PreferencesHttpHandler.class);
      handlerBinder.addBinding().to(ConsoleSettingsHttpHandler.class);
      handlerBinder.addBinding().to(TransactionHttpHandler.class);
      handlerBinder.addBinding().to(WorkflowHttpHandler.class);
      handlerBinder.addBinding().to(ArtifactHttpHandler.class);
      handlerBinder.addBinding().to(WorkflowStatsSLAHttpHandler.class);
      handlerBinder.addBinding().to(AuthorizationHandler.class);
      handlerBinder.addBinding().to(SecureStoreHandler.class);
      handlerBinder.addBinding().to(RemotePrivilegesHandler.class);
      handlerBinder.addBinding().to(RouteConfigHttpHandler.class);
      handlerBinder.addBinding().to(OperationalStatsHttpHandler.class);

      for (Class<? extends HttpHandler> handlerClass : handlerClasses) {
        handlerBinder.addBinding().to(handlerClass);
      }
    }

    @Provides
    @Named(Constants.Service.MASTER_SERVICES_BIND_ADDRESS)
    @SuppressWarnings("unused")
    public InetAddress providesHostname(CConfiguration cConf) {
      String address = cConf.get(Constants.Service.MASTER_SERVICES_BIND_ADDRESS);
      return Networks.resolve(address, new InetSocketAddress("localhost", 0).getAddress());
    }

    /**
     * Provides a supplier of quartz scheduler so that initialization of the scheduler can be done after guice
     * injection. It returns a singleton of Scheduler.
     */
    @Provides
    @SuppressWarnings("unused")
    public Supplier<org.quartz.Scheduler> providesSchedulerSupplier(final DatasetBasedTimeScheduleStore scheduleStore,
                                                                    final CConfiguration cConf) {
      return new Supplier<org.quartz.Scheduler>() {
        private org.quartz.Scheduler scheduler;

        @Override
        public synchronized org.quartz.Scheduler get() {
          try {
            if (scheduler == null) {
              scheduler = getScheduler(scheduleStore, cConf);
            }
            return scheduler;
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      };
    }

    /**
     * Create a quartz scheduler. Quartz factory method is not used, because inflexible in allowing custom jobstore
     * and turning off check for new versions.
     * @param store JobStore.
     * @param cConf CConfiguration.
     * @return an instance of {@link org.quartz.Scheduler}
     */
    private org.quartz.Scheduler getScheduler(JobStore store,
                                              CConfiguration cConf) throws SchedulerException {

      int threadPoolSize = cConf.getInt(Constants.Scheduler.CFG_SCHEDULER_MAX_THREAD_POOL_SIZE);
      ExecutorThreadPool threadPool = new ExecutorThreadPool(threadPoolSize);
      threadPool.initialize();
      String schedulerName = DirectSchedulerFactory.DEFAULT_SCHEDULER_NAME;
      String schedulerInstanceId = DirectSchedulerFactory.DEFAULT_INSTANCE_ID;

      QuartzSchedulerResources qrs = new QuartzSchedulerResources();
      JobRunShellFactory jrsf = new StdJobRunShellFactory();

      qrs.setName(schedulerName);
      qrs.setInstanceId(schedulerInstanceId);
      qrs.setJobRunShellFactory(jrsf);
      qrs.setThreadPool(threadPool);
      qrs.setThreadExecutor(new DefaultThreadExecutor());
      qrs.setJobStore(store);
      qrs.setRunUpdateCheck(false);
      QuartzScheduler qs = new QuartzScheduler(qrs, -1, -1);

      ClassLoadHelper cch = new CascadingClassLoadHelper();
      cch.initialize();

      store.initialize(cch, qs.getSchedulerSignaler());
      org.quartz.Scheduler scheduler = new StdScheduler(qs);

      jrsf.initialize(scheduler);
      qs.initialize();

      return scheduler;
    }
  }
}
