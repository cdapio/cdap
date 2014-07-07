/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.guice;

import com.continuuity.app.authorization.AuthorizationFactory;
import com.continuuity.app.deploy.Manager;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.common.twill.ReactorServiceManager;
import com.continuuity.common.utils.Networks;
import com.continuuity.data.stream.StreamServiceManager;
import com.continuuity.data2.datafabric.dataset.DatasetExecutorServiceManager;
import com.continuuity.explore.service.ExploreServiceManager;
import com.continuuity.gateway.handlers.AppFabricHttpHandler;
import com.continuuity.gateway.handlers.MonitorHandler;
import com.continuuity.gateway.handlers.PingHandler;
import com.continuuity.gateway.handlers.ServiceHttpHandler;
import com.continuuity.http.HttpHandler;
import com.continuuity.internal.app.authorization.PassportAuthorizationFactory;
import com.continuuity.internal.app.deploy.LocalManager;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.runtime.batch.InMemoryTransactionServiceManager;
import com.continuuity.internal.app.runtime.distributed.TransactionServiceManager;
import com.continuuity.internal.app.runtime.schedule.DataSetBasedScheduleStore;
import com.continuuity.internal.app.runtime.schedule.DistributedSchedulerService;
import com.continuuity.internal.app.runtime.schedule.ExecutorThreadPool;
import com.continuuity.internal.app.runtime.schedule.LocalSchedulerService;
import com.continuuity.internal.app.runtime.schedule.Scheduler;
import com.continuuity.internal.app.runtime.schedule.SchedulerService;
import com.continuuity.internal.app.store.MDTBasedStoreFactory;
import com.continuuity.internal.pipeline.SynchronousPipelineFactory;
import com.continuuity.logging.run.AppFabricServiceManager;
import com.continuuity.logging.run.InMemoryDatasetExecutorServiceManager;
import com.continuuity.logging.run.InMemoryExploreServiceManager;
import com.continuuity.logging.run.InMemoryLogSaverServiceManager;
import com.continuuity.logging.run.InMemoryMetricsProcessorServiceManager;
import com.continuuity.logging.run.InMemoryMetricsServiceManager;
import com.continuuity.logging.run.InMemoryStreamServiceManager;
import com.continuuity.logging.run.LogSaverStatusServiceManager;
import com.continuuity.metrics.runtime.MetricsProcessorStatusServiceManager;
import com.continuuity.metrics.runtime.MetricsServiceManager;
import com.continuuity.pipeline.PipelineFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.apache.twill.filesystem.Location;
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

/**
 * AppFabric Service Runtime Module.
 */
public final class AppFabricServiceRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return Modules.combine(new AppFabricServiceModule(),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               bind(SchedulerService.class).to(LocalSchedulerService.class).in(Scopes.SINGLETON);
                               bind(Scheduler.class).to(SchedulerService.class);

                               MapBinder<String, ReactorServiceManager> mapBinder = MapBinder.newMapBinder(
                                 binder(), String.class, ReactorServiceManager.class);
                               mapBinder.addBinding(Constants.Service.LOGSAVER)
                                        .to(InMemoryLogSaverServiceManager.class);
                               mapBinder.addBinding(Constants.Service.TRANSACTION)
                                        .to(InMemoryTransactionServiceManager.class);
                               mapBinder.addBinding(Constants.Service.METRICS_PROCESSOR)
                                        .to(InMemoryMetricsProcessorServiceManager.class);
                               mapBinder.addBinding(Constants.Service.METRICS)
                                        .to(InMemoryMetricsServiceManager.class);
                               mapBinder.addBinding(Constants.Service.APP_FABRIC_HTTP)
                                        .to(AppFabricServiceManager.class);
                               mapBinder.addBinding(Constants.Service.STREAMS)
                                        .to(InMemoryStreamServiceManager.class);
                               mapBinder.addBinding(Constants.Service.DATASET_EXECUTOR)
                                        .to(InMemoryDatasetExecutorServiceManager.class);
                               mapBinder.addBinding(Constants.Service.EXPLORE_HTTP_USER_SERVICE)
                                        .to(InMemoryExploreServiceManager.class);
                             }
                           });
  }

  @Override
  public Module getSingleNodeModules() {

    return Modules.combine(new AppFabricServiceModule(),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               bind(SchedulerService.class).to(LocalSchedulerService.class).in(Scopes.SINGLETON);
                               bind(Scheduler.class).to(SchedulerService.class);

                               MapBinder<String, ReactorServiceManager> mapBinder = MapBinder.newMapBinder(
                                 binder(), String.class, ReactorServiceManager.class);
                               mapBinder.addBinding(Constants.Service.LOGSAVER)
                                        .to(InMemoryLogSaverServiceManager.class);
                               mapBinder.addBinding(Constants.Service.TRANSACTION)
                                        .to(InMemoryTransactionServiceManager.class);
                               mapBinder.addBinding(Constants.Service.METRICS_PROCESSOR)
                                        .to(InMemoryMetricsProcessorServiceManager.class);
                               mapBinder.addBinding(Constants.Service.METRICS)
                                        .to(InMemoryMetricsServiceManager.class);
                               mapBinder.addBinding(Constants.Service.APP_FABRIC_HTTP)
                                        .to(AppFabricServiceManager.class);
                               mapBinder.addBinding(Constants.Service.STREAMS)
                                        .to(InMemoryStreamServiceManager.class);
                               mapBinder.addBinding(Constants.Service.DATASET_EXECUTOR)
                                        .to(InMemoryDatasetExecutorServiceManager.class);
                               mapBinder.addBinding(Constants.Service.EXPLORE_HTTP_USER_SERVICE)
                                        .to(InMemoryExploreServiceManager.class);
                             }
                           });
  }

  @Override
  public Module getDistributedModules() {


    return Modules.combine(new AppFabricServiceModule(),
                           new AbstractModule() {
                             @Override
                             protected void configure() {
                               bind(SchedulerService.class).to(DistributedSchedulerService.class).in(Scopes.SINGLETON);
                               bind(Scheduler.class).to(SchedulerService.class);

                               MapBinder<String, ReactorServiceManager> mapBinder = MapBinder.newMapBinder(
                                 binder(), String.class, ReactorServiceManager.class);
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
                               mapBinder.addBinding(Constants.Service.EXPLORE_HTTP_USER_SERVICE)
                                        .to(ExploreServiceManager.class);
                             }
                           });
  }

  /**
   * Guice module for AppFabricServer. Requires data-fabric related bindings being available.
   */
  private static final class AppFabricServiceModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(PipelineFactory.class).to(SynchronousPipelineFactory.class);

      install(
        new FactoryModuleBuilder()
          .implement(new TypeLiteral<Manager<Location, ApplicationWithPrograms>>() { },
                     new TypeLiteral<LocalManager<Location, ApplicationWithPrograms>>() { })
          .build(new TypeLiteral<ManagerFactory<Location, ApplicationWithPrograms>>() { })
      );

      bind(AuthorizationFactory.class).to(PassportAuthorizationFactory.class);

      bind(StoreFactory.class).to(MDTBasedStoreFactory.class);

      Multibinder<HttpHandler> handlerBinder = Multibinder.newSetBinder(binder(), HttpHandler.class,
                                                                        Names.named("appfabric.http.handler"));
      handlerBinder.addBinding().to(AppFabricHttpHandler.class);
      handlerBinder.addBinding().to(PingHandler.class);
      handlerBinder.addBinding().to(MonitorHandler.class);
      handlerBinder.addBinding().to(ServiceHttpHandler.class);
    }

    @Provides
    @Named(Constants.AppFabric.SERVER_ADDRESS)
    public final InetAddress providesHostname(CConfiguration cConf) {
      return Networks.resolve(cConf.get(Constants.AppFabric.SERVER_ADDRESS),
                              new InetSocketAddress("localhost", 0).getAddress());
    }

    /**
     * Provides a supplier of quartz scheduler so that initialization of the scheduler can be done after guice
     * injection. It returns a singleton of Scheduler.
     */
    @Provides
    public Supplier<org.quartz.Scheduler> providesSchedulerSupplier(final DataSetBasedScheduleStore scheduleStore,
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
     * @throws SchedulerException
     */
    private org.quartz.Scheduler getScheduler(JobStore store,
                                              CConfiguration cConf) throws SchedulerException {

      int threadPoolSize = cConf.getInt(Constants.Scheduler.CFG_SCHEDULER_MAX_THREAD_POOL_SIZE,
                                        Constants.Scheduler.DEFAULT_THREAD_POOL_SIZE);
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
