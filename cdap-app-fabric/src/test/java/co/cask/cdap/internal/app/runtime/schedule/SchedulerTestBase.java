/*
 * Copyright © 2015-2017 Cask Data, Inc.
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
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.ConflictException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.runtime.schedule.trigger.StreamSizeTrigger;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.scheduler.Scheduler;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Base class for scheduler tests
 */
public abstract class SchedulerTestBase {

  static final CConfiguration CCONF = CConfiguration.create();

  private static Scheduler scheduler;
  private static Store store;
  private static NamespaceAdmin namespaceAdmin;
  protected static MetricStore metricStore;
  protected static Injector injector;

  private static final ApplicationId APP_ID = NamespaceId.DEFAULT.app("AppWithStreamSizeSchedule");
  private static final ProgramId PROGRAM_ID = APP_ID.workflow("SampleWorkflow");
  private static final String SCHEDULE_NAME_1 = "SampleSchedule1";
  private static final String SCHEDULE_NAME_2 = "SampleSchedule2";
  private static final ScheduleId SCHEDULE_ID_1 = APP_ID.schedule(SCHEDULE_NAME_1);
  private static final ScheduleId SCHEDULE_ID_2 = APP_ID.schedule(SCHEDULE_NAME_2);
  private static final StreamId STREAM_ID = NamespaceId.DEFAULT.stream("stream");
  private static final ProgramSchedule UPDATE_SCHEDULE_2 =
    new ProgramSchedule(SCHEDULE_NAME_2, "Every 1M", PROGRAM_ID, AppWithStreamSizeSchedule.SCHEDULE_PROPS,
                        new StreamSizeTrigger(STREAM_ID, 1), Collections.<Constraint>emptyList());

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final Supplier<File> TEMP_FOLDER_SUPPLIER = new Supplier<File>() {
    @Override
    public File get() {
      try {
        return tmpFolder.newFolder();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  };

  protected interface StreamMetricsPublisher {
    void increment(long size) throws Exception;
  }

  protected abstract StreamMetricsPublisher createMetricsPublisher(StreamId streamId);

  @BeforeClass
  public static void init() throws Exception {
    injector = AppFabricTestHelper.getInjector(CCONF);
    scheduler = injector.getInstance(Scheduler.class);
    store = injector.getInstance(Store.class);
    metricStore = injector.getInstance(MetricStore.class);
    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    namespaceAdmin.create(NamespaceMeta.DEFAULT);
  }

  @Test
  public void testStreamSizeSchedule() throws Exception {
    // Test the StreamSizeScheduler behavior using notifications
    AppFabricTestHelper.deployApplicationWithManager(AppWithStreamSizeSchedule.class, TEMP_FOLDER_SUPPLIER);
    Assert.assertEquals(ProgramScheduleStatus.SUSPENDED, scheduler.getScheduleStatus(SCHEDULE_ID_1));
    Assert.assertEquals(ProgramScheduleStatus.SUSPENDED, scheduler.getScheduleStatus(SCHEDULE_ID_2));
    scheduler.enableSchedule(SCHEDULE_ID_1);
    scheduler.enableSchedule(SCHEDULE_ID_2);
    Assert.assertEquals(ProgramScheduleStatus.SCHEDULED, scheduler.getScheduleStatus(SCHEDULE_ID_1));
    Assert.assertEquals(ProgramScheduleStatus.SCHEDULED, scheduler.getScheduleStatus(SCHEDULE_ID_2));
    int runs = store.getRuns(PROGRAM_ID, ProgramRunStatus.ALL, 0, Long.MAX_VALUE, 100).size();
    Assert.assertEquals(0, runs);

    StreamMetricsPublisher metricsPublisher = createMetricsPublisher(STREAM_ID);

    // Publish a notification on behalf of the stream with enough data to trigger the execution of the job
    metricsPublisher.increment(1024 * 1024);
    waitForRuns(store, PROGRAM_ID, 1, 15);

    // Trigger both scheduled program
    metricsPublisher.increment(1024 * 1024);
    waitForRuns(store, PROGRAM_ID, 3, 15);

    // Suspend a schedule multiple times, and make sur that it doesn't mess up anything
    scheduler.disableSchedule(SCHEDULE_ID_2);
    try {
      scheduler.disableSchedule(SCHEDULE_ID_2);
      Assert.fail("disable() should have failed because schedule was already disabled");
    } catch (ConflictException e) {
      // expected
    }
    Assert.assertEquals(ProgramScheduleStatus.SUSPENDED, scheduler.getScheduleStatus(SCHEDULE_ID_2));

    // Since schedule 2 is suspended, only the first schedule should get triggered
    metricsPublisher.increment(1024 * 1024);
    waitForRuns(store, PROGRAM_ID, 4, 15);

    // Resume schedule 2
    scheduler.enableSchedule(SCHEDULE_ID_2);
    try {
      scheduler.enableSchedule(SCHEDULE_ID_2);
      Assert.fail("enable() should have failed because schedule was already enabled");
    } catch (ConflictException e) {
      // expected
    }
    Assert.assertEquals(ProgramScheduleStatus.SCHEDULED, scheduler.getScheduleStatus(SCHEDULE_ID_2));

    // Both schedules should be trigger. In particular, the schedule that has just been resumed twice should
    // only trigger once
    metricsPublisher.increment(1024 * 1024);
    waitForRuns(store, PROGRAM_ID, 6, 15);

    // Update the schedule2's data trigger
    // Both schedules should now trigger execution after 1 MB of data received
    scheduler.updateSchedule(UPDATE_SCHEDULE_2);
    metricsPublisher.increment(1024 * 1024);
    waitForRuns(store, PROGRAM_ID, 8, 15);

    scheduler.disableSchedule(SCHEDULE_ID_1);
    scheduler.disableSchedule(SCHEDULE_ID_2);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    namespaceAdmin.delete(NamespaceId.DEFAULT);
  }

  /**
   * Waits for the given program ran for the given number of times.
   */
  private void waitForRuns(final Store store, final ProgramId programId,
                           int expectedRuns, long timeoutSeconds) throws Exception {
    Tasks.waitFor(expectedRuns, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return store.getRuns(programId, ProgramRunStatus.COMPLETED, 0, Long.MAX_VALUE, 100).size();
      }
    }, timeoutSeconds, TimeUnit.SECONDS, 50, TimeUnit.MILLISECONDS);
  }
}
