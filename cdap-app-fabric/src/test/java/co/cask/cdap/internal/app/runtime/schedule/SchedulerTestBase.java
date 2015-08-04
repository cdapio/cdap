/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.AppWithStreamSizeSchedule;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.NamespaceCannotBeDeletedException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.namespace.NamespaceAdmin;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Predicate;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Base class for scheduler tests
 */
public abstract class SchedulerTestBase {

  protected static final CConfiguration CCONF = CConfiguration.create();

  private static StreamSizeScheduler streamSizeScheduler;
  private static Store store;
  private static NamespaceAdmin namespaceAdmin;
  private static ProgramRuntimeService runtimeService;
  protected static MetricStore metricStore;
  protected static Injector injector;

  private static final Id.Application APP_ID = new Id.Application(Constants.DEFAULT_NAMESPACE_ID,
                                                                  "AppWithStreamSizeSchedule");
  private static final Id.Program PROGRAM_ID = new Id.Program(APP_ID, ProgramType.WORKFLOW, "SampleWorkflow");
  private static final String SCHEDULE_NAME_1 = "SampleSchedule1";
  private static final String SCHEDULE_NAME_2 = "SampleSchedule2";
  private static final SchedulableProgramType PROGRAM_TYPE = SchedulableProgramType.WORKFLOW;
  private static final Id.Stream STREAM_ID = Id.Stream.from(Constants.DEFAULT_NAMESPACE_ID, "stream");
  private static final Schedule UPDATE_SCHEDULE_2 =
    Schedules.createDataSchedule(SCHEDULE_NAME_2, "Every 1M", Schedules.Source.STREAM, STREAM_ID.getId(), 1);

  protected interface StreamMetricsPublisher {
    void increment(long size) throws Exception;
  }

  protected abstract StreamMetricsPublisher createMetricsPublisher(Id.Stream streamId);

  @BeforeClass
  public static void init() throws Exception {
    injector = AppFabricTestHelper.getInjector(CCONF);
    PreferencesStore preferencesStore = injector.getInstance(PreferencesStore.class);
    streamSizeScheduler = injector.getInstance(StreamSizeScheduler.class);
    store = injector.getInstance(Store.class);
    metricStore = injector.getInstance(MetricStore.class);
    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    namespaceAdmin.createNamespace(Constants.DEFAULT_NAMESPACE_META);
    runtimeService = injector.getInstance(ProgramRuntimeService.class);
  }

  @Test
  public void testStreamSizeSchedule() throws Exception {
    // Test the StreamSizeScheduler behavior using notifications
    AppFabricTestHelper.deployApplication(AppWithStreamSizeSchedule.class);
    Assert.assertEquals(Scheduler.ScheduleState.SUSPENDED,
                        streamSizeScheduler.scheduleState(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_1));
    Assert.assertEquals(Scheduler.ScheduleState.SUSPENDED,
                        streamSizeScheduler.scheduleState(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_2));
    streamSizeScheduler.resumeSchedule(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_1);
    streamSizeScheduler.resumeSchedule(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_2);
    Assert.assertEquals(Scheduler.ScheduleState.SCHEDULED,
                        streamSizeScheduler.scheduleState(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_1));
    Assert.assertEquals(Scheduler.ScheduleState.SCHEDULED,
                        streamSizeScheduler.scheduleState(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_2));
    int runs = store.getRuns(PROGRAM_ID, ProgramRunStatus.ALL, 0, Long.MAX_VALUE, 100).size();
    Assert.assertEquals(0, runs);

    StreamMetricsPublisher metricsPublisher = createMetricsPublisher(STREAM_ID);

    // Publish a notification on behalf of the stream with enough data to trigger the execution of the job
    metricsPublisher.increment(1024 * 1024);
    waitForRuns(store, PROGRAM_ID, 1, 5);
    waitUntilFinished(runtimeService, PROGRAM_ID, 5);

    // Trigger both scheduled program
    metricsPublisher.increment(1024 * 1024);
    waitForRuns(store, PROGRAM_ID, 3, 5);

    // Suspend a schedule multiple times, and make sur that it doesn't mess up anything
    streamSizeScheduler.suspendSchedule(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_2);
    streamSizeScheduler.suspendSchedule(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_2);
    Assert.assertEquals(Scheduler.ScheduleState.SUSPENDED,
                        streamSizeScheduler.scheduleState(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_2));

    // Since schedule 2 is suspended, only the first schedule should get triggered
    metricsPublisher.increment(1024 * 1024);
    waitForRuns(store, PROGRAM_ID, 4, 5);

    // Resume schedule 2
    streamSizeScheduler.resumeSchedule(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_2);
    streamSizeScheduler.resumeSchedule(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_2);
    Assert.assertEquals(Scheduler.ScheduleState.SCHEDULED,
                        streamSizeScheduler.scheduleState(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_2));

    // Both schedules should be trigger. In particular, the schedule that has just been resumed twice should
    // only trigger once
    metricsPublisher.increment(1024 * 1024);
    waitForRuns(store, PROGRAM_ID, 6, 5);

    // Update the schedule2's data trigger
    // Both schedules should now trigger execution after 1 MB of data received
    streamSizeScheduler.updateSchedule(PROGRAM_ID, PROGRAM_TYPE, UPDATE_SCHEDULE_2);
    metricsPublisher.increment(1024 * 1024);
    waitForRuns(store, PROGRAM_ID, 8, 5);

    streamSizeScheduler.suspendSchedule(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_1);
    streamSizeScheduler.suspendSchedule(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_2);
    streamSizeScheduler.deleteSchedules(PROGRAM_ID, PROGRAM_TYPE);
    waitUntilFinished(runtimeService, PROGRAM_ID, 10);
  }

  @AfterClass
  public static void tearDown() throws NotFoundException, NamespaceCannotBeDeletedException {
    namespaceAdmin.deleteNamespace(Constants.DEFAULT_NAMESPACE_ID);
  }

  /**
   * Waits until there is nothing running for the given program.
   */
  private void waitUntilFinished(final ProgramRuntimeService runtimeService,
                                 final Id.Program program, long maxWaitSeconds) throws Exception {
    Tasks.waitFor(false, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return isProgramRunning(runtimeService, program);
      }
    }, maxWaitSeconds, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
  }

  /**
   * Waits for the given program ran for the given number of times.
   */
  private void waitForRuns(final Store store, final Id.Program programId,
                           int expectedRuns, long timeoutSeconds) throws Exception {
    Tasks.waitFor(expectedRuns, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return store.getRuns(programId, ProgramRunStatus.COMPLETED, 0, Long.MAX_VALUE, 100).size();
      }
    }, timeoutSeconds, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
  }


  private boolean isProgramRunning(ProgramRuntimeService runtimeService, final Id.Program program) {
    return runtimeService.checkAnyRunning(new Predicate<Id.Program>() {
      @Override
      public boolean apply(Id.Program programId) {
        return programId.equals(program);
      }
    }, ProgramType.values());
  }
}
