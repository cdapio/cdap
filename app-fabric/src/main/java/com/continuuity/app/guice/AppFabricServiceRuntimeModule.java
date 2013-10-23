/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.guice;

import com.continuuity.app.authorization.AuthorizationFactory;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.runtime.RuntimeModule;
import com.continuuity.common.utils.Networks;
import com.continuuity.internal.app.authorization.PassportAuthorizationFactory;
import com.continuuity.internal.app.deploy.SyncManagerFactory;
import com.continuuity.internal.app.runtime.schedule.DataSetBasedScheduleStore;
import com.continuuity.internal.app.runtime.schedule.DefaultSchedulerService;
import com.continuuity.internal.app.runtime.schedule.ExecutorThreadPool;
import com.continuuity.internal.app.runtime.schedule.Scheduler;
import com.continuuity.internal.app.runtime.schedule.SchedulerService;
import com.continuuity.internal.app.services.AppFabricServiceFactory;
import com.continuuity.internal.app.services.DefaultAppFabricService;
import com.continuuity.internal.app.store.MDTBasedStoreFactory;
import com.continuuity.internal.pipeline.SynchronousPipelineFactory;
import com.continuuity.pipeline.PipelineFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Named;
import org.quartz.SchedulerException;
import org.quartz.core.JobRunShellFactory;
import org.quartz.core.QuartzScheduler;
import org.quartz.core.QuartzSchedulerResources;
import org.quartz.impl.DefaultThreadExecutor;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.impl.SchedulerRepository;
import org.quartz.impl.StdJobRunShellFactory;
import org.quartz.impl.StdScheduler;
import org.quartz.simpl.CascadingClassLoadHelper;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 *
 */
public final class AppFabricServiceRuntimeModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new AppFabricServiceModule();
  }

  @Override
  public Module getSingleNodeModules() {
    return new AppFabricServiceModule();
  }

  @Override
  public Module getDistributedModules() {
    return new AppFabricServiceModule();
  }

  /**
   * Guice module for AppFabricServer. Requires data-fabric related bindings being available.
   */
  // Note: Ideally this should be private module, but gateway and test cases uses some of the internal bindings.
  private static final class AppFabricServiceModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(new TypeLiteral<PipelineFactory<?>>(){}).to(new TypeLiteral<SynchronousPipelineFactory<?>>(){});
      bind(ManagerFactory.class).to(SyncManagerFactory.class);

      bind(AuthorizationFactory.class).to(PassportAuthorizationFactory.class);

      install(
        new FactoryModuleBuilder()
          .implement(AppFabricService.Iface.class, DefaultAppFabricService.class)
          .build(AppFabricServiceFactory.class)
      );

      bind(StoreFactory.class).to(MDTBasedStoreFactory.class);
      bind(SchedulerService.class).to(DefaultSchedulerService.class);
      bind(Scheduler.class).to(SchedulerService.class);
    }

    @Provides
    @Named(Constants.AppFabric.SERVER_ADDRESS)
    public InetAddress providesHostname(CConfiguration cConf) {
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

      SchedulerRepository schedRep = SchedulerRepository.getInstance();
      qs.addNoGCObject(schedRep); // prevents the repository from being garbage collected
      schedRep.bind(scheduler);

      return scheduler;
    }
  }
}
