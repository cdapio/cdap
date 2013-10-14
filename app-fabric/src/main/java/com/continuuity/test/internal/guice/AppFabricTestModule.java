/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.test.internal.guice;

import com.continuuity.api.schedule.Schedule;
import com.continuuity.app.Id;
import com.continuuity.app.guice.AppFabricServiceRuntimeModule;
import com.continuuity.app.guice.ProgramRunnerRuntimeModule;
import com.continuuity.app.program.Type;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.internal.app.runtime.schedule.ScheduledRuntime;
import com.continuuity.internal.app.runtime.schedule.Scheduler;
import com.continuuity.internal.app.services.DefaultAppFabricService;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.weave.filesystem.LocalLocationFactory;
import com.continuuity.weave.filesystem.LocationFactory;
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
    install(new ConfigModule(cConf, hConf));
    install(new IOModule());
    install(new DiscoveryRuntimeModule().getInMemoryModules());
    install(new AppFabricServiceRuntimeModule().getInMemoryModules());
    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(Scheduler.class).annotatedWith(Assisted.class).toInstance(createNoopScheduler());
        bind(AppFabricService.Iface.class).to(DefaultAppFabricService.class);
        expose(AppFabricService.Iface.class);
      }
    });
    install(new ProgramRunnerRuntimeModule().getInMemoryModules());
    install(new MetricsClientRuntimeModule().getNoopModules());
    bind(LocationFactory.class).toInstance(new LocalLocationFactory());
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
      public ScheduleState scheduleState(String scheduleId){
        return ScheduleState.NOT_FOUND;
      }
    };
  }
}
