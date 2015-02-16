/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.test.internal.guice;

import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.guice.ServiceStoreModules;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.config.guice.ConfigStoreModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.stream.service.StreamServiceRuntimeModule;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.gateway.auth.AuthModule;
import co.cask.cdap.internal.app.runtime.schedule.ScheduledRuntime;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.metrics.guice.MetricsHandlerModule;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import co.cask.cdap.proto.Id;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.assistedinject.Assisted;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.util.List;

/**
 *
 */
public final class AppFabricTestModule extends AbstractModule {

  private final CConfiguration cConf;
  private final Configuration hConf;

  public AppFabricTestModule(CConfiguration configuration) {
    this.cConf = configuration;

    File localDataDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR));

    hConf = new Configuration();
    hConf.addResource("mapred-site-local.xml");
    hConf.reloadConfiguration();
    hConf.set("hadoop.tmp.dir", new File(localDataDir, cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsolutePath());
    hConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    hConf.set(Constants.AppFabric.OUTPUT_DIR, cConf.get(Constants.AppFabric.OUTPUT_DIR));
  }

  @Override
  protected void configure() {
    install(new DataFabricModules().getInMemoryModules());
    install(new DataSetsModules().getLocalModule());
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
    install(new LocationRuntimeModule().getInMemoryModules());
    install(new LoggingModules().getInMemoryModules());
    install(new MetricsHandlerModule());
    install(new MetricsClientRuntimeModule().getInMemoryModules());
    install(new ExploreClientModule());
    install(new NotificationFeedServiceRuntimeModule().getInMemoryModules());
    install(new ConfigStoreModule().getInMemoryModule());
    install(new StreamAdminModules().getInMemoryModules());
    install(new StreamServiceRuntimeModule().getInMemoryModules());
  }

  private Scheduler createNoopScheduler() {
    return new Scheduler() {
      @Override
      public void schedule(Id.Program program, SchedulableProgramType programType, Schedule schedule) {
      }

      @Override
      public void schedule(Id.Program program, SchedulableProgramType programType, Iterable<Schedule> schedules) {
      }

      @Override
      public List<ScheduledRuntime> nextScheduledRuntime(Id.Program program, SchedulableProgramType programType) {
        return ImmutableList.of();
      }

      @Override
      public List<String> getScheduleIds(Id.Program program, SchedulableProgramType programType) {
        return ImmutableList.of();
      }

      @Override
      public void suspendSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName) {
      }

      @Override
      public void resumeSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName) {
      }

      @Override
      public void deleteSchedule(Id.Program program, SchedulableProgramType programType, String scheduleName) {
      }

      @Override
      public void deleteSchedules(Id.Program programId, SchedulableProgramType programType) {
      }

      @Override
      public ScheduleState scheduleState(Id.Program program, SchedulableProgramType programType, String scheduleName) {
        return ScheduleState.NOT_FOUND;
      }
    };
  }
}
