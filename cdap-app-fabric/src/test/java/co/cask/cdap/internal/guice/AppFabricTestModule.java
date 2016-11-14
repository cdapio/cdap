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

package co.cask.cdap.internal.guice;

import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.guice.AuthorizationModule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.guice.ServiceStoreModules;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.config.guice.ConfigStoreModule;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.TransactionExecutorModule;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.stream.service.StreamServiceRuntimeModule;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.gateway.handlers.meta.RemoteSystemOperationsServiceModule;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.internal.app.runtime.schedule.SchedulerException;
import co.cask.cdap.logging.guice.LogReaderRuntimeModules;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.metadata.MetadataServiceModule;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.metrics.guice.MetricsHandlerModule;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.cdap.proto.ScheduledRuntime;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.guice.SecureStoreModules;
import co.cask.cdap.store.guice.NamespaceStoreModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.assistedinject.Assisted;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
public final class AppFabricTestModule extends AbstractModule {

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final SConfiguration sConf;

  public AppFabricTestModule(CConfiguration configuration) {
    this(configuration, null);
  }

  public AppFabricTestModule(CConfiguration cConf, @Nullable SConfiguration sConf) {
    this.cConf = cConf;

    File localDataDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR));

    hConf = new Configuration();
    hConf.addResource("mapred-site-local.xml");
    hConf.reloadConfiguration();
    hConf.set("hadoop.tmp.dir", new File(localDataDir, cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsolutePath());
    hConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    hConf.set(Constants.AppFabric.OUTPUT_DIR, cConf.get(Constants.AppFabric.OUTPUT_DIR));

    this.sConf = sConf == null ? SConfiguration.create() : sConf;
  }

  @Override
  protected void configure() {
    install(new DataFabricModules().getInMemoryModules());
    install(new DataSetsModules().getStandaloneModules());
    install(new TransactionExecutorModule());
    install(new DataSetServiceModules().getInMemoryModules());
    install(new ConfigModule(cConf, hConf, sConf));
    install(new IOModule());
    install(new DiscoveryRuntimeModule().getInMemoryModules());
    install(new AppFabricServiceRuntimeModule().getInMemoryModules());
    install(new ServiceStoreModules().getInMemoryModules());
    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(Scheduler.class).annotatedWith(Assisted.class).toInstance(createNoopScheduler());
      }
    });
    install(new ProgramRunnerRuntimeModule().getInMemoryModules());
    install(new NonCustomLocationUnitTestModule().getModule());
    install(new LoggingModules().getInMemoryModules());
    install(new LogReaderRuntimeModules().getInMemoryModules());
    install(new MetricsHandlerModule());
    install(new MetricsClientRuntimeModule().getInMemoryModules());
    install(new ExploreClientModule());
    install(new NotificationFeedServiceRuntimeModule().getInMemoryModules());
    install(new NotificationServiceRuntimeModule().getInMemoryModules());
    install(new ConfigStoreModule().getInMemoryModule());
    install(new ViewAdminModules().getInMemoryModules());
    install(new StreamAdminModules().getInMemoryModules());
    install(new StreamServiceRuntimeModule().getInMemoryModules());
    install(new NamespaceStoreModule().getStandaloneModules());
    install(new MetadataServiceModule());
    install(new RemoteSystemOperationsServiceModule());
    install(new AuthorizationModule());
    // we want to use RemotePrivilegesFetcher in this module, since app fabric service is started
    install(new AuthorizationEnforcementModule().getStandaloneModules());
    install(new SecureStoreModules().getInMemoryModules());
  }

  private Scheduler createNoopScheduler() {
    return new Scheduler() {
      @Override
      public void schedule(ProgramId program, SchedulableProgramType programType, Schedule schedule) {
      }

      @Override
      public void schedule(ProgramId program, SchedulableProgramType programType, Schedule schedule,
                           Map<String, String> properties) {
      }

      @Override
      public void schedule(ProgramId program, SchedulableProgramType programType, Iterable<Schedule> schedules) {
      }

      @Override
      public void schedule(ProgramId program, SchedulableProgramType programType, Iterable<Schedule> schedules,
                           Map<String, String> properties) {
      }

      @Override
      public List<ScheduledRuntime> previousScheduledRuntime(ProgramId program, SchedulableProgramType programType) {
        return ImmutableList.of();
      }

      @Override
      public List<ScheduledRuntime> nextScheduledRuntime(ProgramId program, SchedulableProgramType programType) {
        return ImmutableList.of();
      }

      @Override
      public List<String> getScheduleIds(ProgramId program, SchedulableProgramType programType) {
        return ImmutableList.of();
      }

      @Override
      public void suspendSchedule(ProgramId program, SchedulableProgramType programType, String scheduleName) {
      }

      @Override
      public void resumeSchedule(ProgramId program, SchedulableProgramType programType, String scheduleName) {
      }

      @Override
      public void updateSchedule(ProgramId program, SchedulableProgramType programType, Schedule schedule) {
      }

      @Override
      public void updateSchedule(ProgramId program, SchedulableProgramType programType, Schedule schedule,
                                 Map<String, String> properties) {
      }

      @Override
      public void deleteSchedule(ProgramId program, SchedulableProgramType programType, String scheduleName) {
      }

      @Override
      public void deleteSchedules(ProgramId programId, SchedulableProgramType programType) {
      }

      @Override
      public void deleteAllSchedules(NamespaceId namespaceId) throws SchedulerException {
      }

      @Override
      public ScheduleState scheduleState(ProgramId program, SchedulableProgramType programType, String scheduleName) {
        return ScheduleState.NOT_FOUND;
      }
    };
  }
}
