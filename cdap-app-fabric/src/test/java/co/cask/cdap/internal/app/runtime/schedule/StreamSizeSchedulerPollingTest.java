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
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.MetricValue;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.config.PreferencesStore;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.notifications.service.NotificationService;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.internal.AppFabricTestHelper;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
@Category(SlowTests.class)
public class StreamSizeSchedulerPollingTest {
  public static StreamSizeScheduler streamSizeScheduler;
  public static NotificationFeedManager notificationFeedManager;
  public static NotificationService notificationService;
  public static Store store;
  public static MetricStore metricStore;

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final Id.Namespace NAMESPACE = new Id.Namespace(Constants.DEFAULT_NAMESPACE);
  private static final Id.Application APP_ID = new Id.Application(NAMESPACE, "AppWithStreamSizeSchedule");
  private static final Id.Program PROGRAM_ID = new Id.Program(APP_ID, "SampleWorkflow");
  private static final String SCHEDULE_NAME_1 = "SampleSchedule1";
  private static final String SCHEDULE_NAME_2 = "SampleSchedule2";
  private static final SchedulableProgramType PROGRAM_TYPE = SchedulableProgramType.WORKFLOW;
  private static final Id.Stream STREAM_ID = Id.Stream.from(NAMESPACE, "stream");
  private static final Schedule UPDATE_SCHEDULE_2 =
    Schedules.createDataSchedule(SCHEDULE_NAME_2, "Every 1M", Schedules.Source.STREAM, STREAM_ID.getName(), 1);

  @BeforeClass
  public static void set() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setLong(Constants.Notification.Stream.STREAM_SIZE_SCHEDULE_POLLING_DELAY, 1);
    PreferencesStore preferencesStore = AppFabricTestHelper.getInjector(cConf).getInstance(PreferencesStore.class);
    Map<String, String> properties = ImmutableMap.of(ProgramOptionConstants.CONCURRENT_RUNS_ENABLED, "true");
    preferencesStore.setProperties(NAMESPACE.getId(), APP_ID.getId(), properties);
    notificationFeedManager = AppFabricTestHelper.getInjector(cConf).getInstance(NotificationFeedManager.class);
    notificationService = AppFabricTestHelper.getInjector(cConf).getInstance(NotificationService.class);
    streamSizeScheduler = AppFabricTestHelper.getInjector(cConf).getInstance(StreamSizeScheduler.class);
    StoreFactory storeFactory = AppFabricTestHelper.getInjector(cConf).getInstance(StoreFactory.class);
    store = storeFactory.create();
    metricStore = AppFabricTestHelper.getInjector(cConf).getInstance(MetricStore.class);
  }

  @Test
  public void testStreamSizeSchedule() throws Exception {
    // Test the StreamSizeScheduler behavior using polling

    AppFabricTestHelper.deployApplication(AppWithStreamSizeSchedule.class);
    Assert.assertEquals(Scheduler.ScheduleState.SCHEDULED,
                        streamSizeScheduler.scheduleState(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_1));

    int runs = store.getRuns(PROGRAM_ID, ProgramRunStatus.ALL, Long.MIN_VALUE, Long.MAX_VALUE, 100).size();
    Assert.assertEquals(0, runs);

    // By updating the stream metrics directly, no notification will be triggered.
    // Hence we can test the polling logic
    metricStore.add(new MetricValue(ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, STREAM_ID.getNamespaceId(),
                                                    Constants.Metrics.Tag.STREAM, STREAM_ID.getName()),
                                    "collect.bytes", TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                                    1024 * 1024, MetricType.COUNTER));

    waitForRuns(PROGRAM_ID, 1);

    // Make sure that we don't have any more runs
    TimeUnit.SECONDS.sleep(5);
    runs = store.getRuns(PROGRAM_ID, ProgramRunStatus.ALL, Long.MIN_VALUE, Long.MAX_VALUE, 100).size();
    Assert.assertEquals(1, runs);

    // Suspend the schedule - this should suspend the polling of the stream,
    // since all schedules for the stream are inactive now
    streamSizeScheduler.suspendSchedule(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_1);
    streamSizeScheduler.suspendSchedule(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_2);
    Assert.assertEquals(Scheduler.ScheduleState.SUSPENDED, streamSizeScheduler.scheduleState(PROGRAM_ID, PROGRAM_TYPE,
                                                                                             SCHEDULE_NAME_1));
    Assert.assertEquals(Scheduler.ScheduleState.SUSPENDED,
                        streamSizeScheduler.scheduleState(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_2));


    // We fake the writing of another 1MB of data, triggering both schedules
    metricStore.add(new MetricValue(ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, STREAM_ID.getNamespaceId(),
                                                    Constants.Metrics.Tag.STREAM, STREAM_ID.getName()),
                                    "collect.bytes", TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                                    1024 * 1024, MetricType.COUNTER));

    // Should not have any run when the schedule is suspended
    TimeUnit.SECONDS.sleep(5);
    runs = store.getRuns(PROGRAM_ID, ProgramRunStatus.ALL, Long.MIN_VALUE, Long.MAX_VALUE, 100).size();
    Assert.assertEquals(1, runs);

    // Resume schedule - polling the stream should be triggered, and a run should then happen
    streamSizeScheduler.resumeSchedule(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_1);
    Assert.assertEquals(Scheduler.ScheduleState.SCHEDULED,
                        streamSizeScheduler.scheduleState(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_1));
    waitForRuns(PROGRAM_ID, 2);

    // Resume the second schedule, that waits for 2 MB
    streamSizeScheduler.resumeSchedule(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_2);
    Assert.assertEquals(Scheduler.ScheduleState.SCHEDULED,
                        streamSizeScheduler.scheduleState(PROGRAM_ID, PROGRAM_TYPE, SCHEDULE_NAME_2));
    waitForRuns(PROGRAM_ID, 3);

    // Update the schedule2's data trigger
    // Both schedules should now trigger execution after 1 MB of data received
    streamSizeScheduler.updateSchedule(PROGRAM_ID, PROGRAM_TYPE, UPDATE_SCHEDULE_2);
    metricStore.add(new MetricValue(ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, STREAM_ID.getNamespaceId(),
                                                    Constants.Metrics.Tag.STREAM, STREAM_ID.getName()),
                                    "collect.bytes", TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()),
                                    1024 * 1024, MetricType.COUNTER));
    waitForRuns(PROGRAM_ID, 5);
  }

  private void waitForRuns(Id.Program programId, int expectedRuns) throws Exception {
    int runs;
    long initTime = System.currentTimeMillis();
    while (System.currentTimeMillis() < initTime + TimeUnit.SECONDS.toMillis(5)) {
      runs = store.getRuns(programId, ProgramRunStatus.ALL, Long.MIN_VALUE, Long.MAX_VALUE, 100).size();
      try {
        Assert.assertEquals(expectedRuns, runs);
        return;
      } catch (Throwable t) {
        TimeUnit.MILLISECONDS.sleep(100);
      }
    }
    Assert.fail("Time out");
  }
}
