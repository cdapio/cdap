/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.test.internal.guice;

import com.continuuity.api.schedule.Schedule;
import com.continuuity.app.Id;
import com.continuuity.app.guice.AppFabricServiceRuntimeModule;
import com.continuuity.app.guice.ProgramRunnerRuntimeModule;
import com.continuuity.app.guice.ServiceStoreModules;
import com.continuuity.app.program.Type;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data.runtime.DataSetServiceModules;
import com.continuuity.data.runtime.DataSetsModules;
import com.continuuity.gateway.auth.AuthModule;
import com.continuuity.internal.app.runtime.schedule.ScheduledRuntime;
import com.continuuity.internal.app.runtime.schedule.Scheduler;
import com.continuuity.logging.guice.LoggingModules;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.metrics.guice.MetricsHandlerModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.assistedinject.Assisted;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 *
 */
public final class AppFabricTestModule extends AbstractModule {

  private final CConfiguration cConf;
  private final Configuration hConf;

  public AppFabricTestModule(CConfiguration configuration) {
    this.cConf = configuration;
    hConf = new Configuration();
    hConf.addResource("mapred-site-local.xml");
    hConf.reloadConfiguration();
  }

  @Override
  protected void configure() {
    install(new DataFabricModules().getInMemoryModules());
    install(new DataSetsModules().getInMemoryModule());
    install(new DataSetServiceModules().getInMemoryModule());
    install(new ConfigModule(cConf, hConf));
    install(new IOModule());
    install(new AuthModule());
    install(new DiscoveryRuntimeModule().getInMemoryModules());
    install(new AppFabricServiceRuntimeModule().getInMemoryModules());
    install(new ServiceStoreModules().getInMemoryModule());
    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(Scheduler.class).annotatedWith(Assisted.class).toInstance(createNoopScheduler());
      }
    });
    install(new ProgramRunnerRuntimeModule().getInMemoryModules());
    install(new MetricsClientRuntimeModule().getNoopModules());
    install(new LocationRuntimeModule().getInMemoryModules());
    install(new LoggingModules().getInMemoryModules());
    install(new MetricsHandlerModule());
  }

  private Scheduler createNoopScheduler() {
    return new Scheduler() {
      @Override
      public void schedule(Id.Program program, Type programType, Iterable<Schedule> schedules) {
      }

      @Override
      public List<ScheduledRuntime> nextScheduledRuntime(Id.Program program, Type programType) {
        return ImmutableList.of();
      }

      @Override
      public List<String> getScheduleIds(Id.Program program, Type programType) {
        return ImmutableList.of();
      }

      @Override
      public void suspendSchedule(String scheduleId) {
      }

      @Override
      public void resumeSchedule(String scheduleId) {
      }

      @Override
      public void deleteSchedules(Id.Program programId, Type programType, List<String> scheduleIds) {
      }

      @Override
      public ScheduleState scheduleState(String scheduleId) {
        return ScheduleState.NOT_FOUND;
      }
    };
  }
}
