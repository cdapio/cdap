/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.scheduler;

import co.cask.cdap.AppWithFrequentScheduledWorkflows;
import co.cask.cdap.api.Config;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.ConflictException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data.runtime.DynamicTransactionExecutorFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import co.cask.cdap.internal.app.runtime.schedule.queue.JobQueueDataset;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.internal.app.runtime.schedule.trigger.PartitionTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
import co.cask.cdap.proto.id.TopicId;
import co.cask.cdap.proto.id.WorkflowId;
import co.cask.cdap.test.XSlowTests;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CoreSchedulerServiceTest extends AppFabricTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(CoreSchedulerServiceTest.class);

  private static final NamespaceId NS_ID = new NamespaceId("schedtest");
  private static final ApplicationId APP1_ID = NS_ID.app("app1");
  private static final ApplicationId APP2_ID = NS_ID.app("app2");
  private static final WorkflowId PROG1_ID = APP1_ID.workflow("wf1");
  private static final WorkflowId PROG2_ID = APP2_ID.workflow("wf2");
  private static final WorkflowId PROG11_ID = APP1_ID.workflow("wf11");
  private static final ScheduleId PSCHED1_ID = APP1_ID.schedule("psched1");
  private static final ScheduleId PSCHED2_ID = APP2_ID.schedule("psched2");
  private static final ScheduleId TSCHED1_ID = APP1_ID.schedule("tsched1");
  private static final ScheduleId TSCHED11_ID = APP1_ID.schedule("tsched11");
  private static final DatasetId DS1_ID = NS_ID.dataset("pfs1");
  private static final DatasetId DS2_ID = NS_ID.dataset("pfs2");
  private static final ApplicationId APP_ID = NamespaceId.DEFAULT.app("AppWithFrequentScheduledWorkflows");
  private static final ProgramId WORKFLOW_1 = APP_ID.program(ProgramType.WORKFLOW,
                                                             AppWithFrequentScheduledWorkflows.SOME_WORKFLOW);
  private static final ProgramId WORKFLOW_2 = APP_ID.program(ProgramType.WORKFLOW,
                                                             AppWithFrequentScheduledWorkflows.ANOTHER_WORKFLOW);
  private static final ProgramId SCHEDULED_WORKFLOW_1 =
    APP_ID.program(ProgramType.WORKFLOW, AppWithFrequentScheduledWorkflows.SCHEDULED_WORKFLOW_1);
  private static final ProgramId SCHEDULED_WORKFLOW_2 =
    APP_ID.program(ProgramType.WORKFLOW, AppWithFrequentScheduledWorkflows.SCHEDULED_WORKFLOW_2);

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private static final Gson GSON = new Gson();

  private static MessagingService messagingService;
  private static Store store;
  private static TopicId dataEventTopic;

  private static Scheduler scheduler;

  @BeforeClass
  public static void beforeClass() throws Throwable {
    AppFabricTestBase.beforeClass();
    scheduler = getInjector().getInstance(Scheduler.class);
    if (scheduler instanceof Service) {
      ((Service) scheduler).startAndWait();
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    AppFabricTestBase.afterClass();
    if (scheduler instanceof Service) {
      ((Service) scheduler).stopAndWait();
    }
  }

  @Test
  public void addListDeleteSchedules() throws Exception {
    // verify that list returns nothing
    Assert.assertTrue(scheduler.listSchedules(APP1_ID).isEmpty());
    Assert.assertTrue(scheduler.listSchedules(PROG1_ID).isEmpty());

    // add a schedule for app1
    ProgramSchedule tsched1 = new ProgramSchedule("tsched1", "one time schedule", PROG1_ID,
                                                  ImmutableMap.of("prop1", "nn"),
                                                  new TimeTrigger("* * ? * 1"), ImmutableList.<Constraint>of());
    scheduler.addSchedule(tsched1);
    Assert.assertEquals(tsched1, scheduler.getSchedule(TSCHED1_ID));
    Assert.assertEquals(ImmutableList.of(tsched1), scheduler.listSchedules(APP1_ID));
    Assert.assertEquals(ImmutableList.of(tsched1), scheduler.listSchedules(PROG1_ID));

    // add three more schedules, one for the same program, one for the same app, one for another app
    ProgramSchedule psched1 = new ProgramSchedule("psched1", "one partition schedule", PROG1_ID,
                                                  ImmutableMap.of("prop3", "abc"),
                                                  new PartitionTrigger(DS1_ID, 1), ImmutableList.<Constraint>of());
    ProgramSchedule tsched11 = new ProgramSchedule("tsched11", "two times schedule", PROG11_ID,
                                                   ImmutableMap.of("prop2", "xx"),
                                                   new TimeTrigger("* * ? * 1,2"), ImmutableList.<Constraint>of());
    ProgramSchedule psched2 = new ProgramSchedule("psched2", "two partition schedule", PROG2_ID,
                                                  ImmutableMap.of("propper", "popper"),
                                                  new PartitionTrigger(DS2_ID, 2), ImmutableList.<Constraint>of());
    scheduler.addSchedules(ImmutableList.of(psched1, tsched11, psched2));
    Assert.assertEquals(psched1, scheduler.getSchedule(PSCHED1_ID));
    Assert.assertEquals(tsched11, scheduler.getSchedule(TSCHED11_ID));
    Assert.assertEquals(psched2, scheduler.getSchedule(PSCHED2_ID));

    // list by app and program
    Assert.assertEquals(ImmutableList.of(psched1, tsched1), scheduler.listSchedules(PROG1_ID));
    Assert.assertEquals(ImmutableList.of(tsched11), scheduler.listSchedules(PROG11_ID));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(PROG2_ID));
    Assert.assertEquals(ImmutableList.of(psched1, tsched1, tsched11), scheduler.listSchedules(APP1_ID));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(APP2_ID));

    // delete one schedule
    scheduler.deleteSchedule(TSCHED1_ID);
    verifyNotFound(scheduler, TSCHED1_ID);
    Assert.assertEquals(ImmutableList.of(psched1), scheduler.listSchedules(PROG1_ID));
    Assert.assertEquals(ImmutableList.of(tsched11), scheduler.listSchedules(PROG11_ID));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(PROG2_ID));
    Assert.assertEquals(ImmutableList.of(psched1, tsched11), scheduler.listSchedules(APP1_ID));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(APP2_ID));

    // attempt to delete it again along with another one that exists
    try {
      scheduler.deleteSchedules(ImmutableList.of(TSCHED1_ID, TSCHED11_ID));
      Assert.fail("expected NotFoundException");
    } catch (NotFoundException e) {
      // expected
    }
    Assert.assertEquals(ImmutableList.of(psched1), scheduler.listSchedules(PROG1_ID));
    Assert.assertEquals(ImmutableList.of(tsched11), scheduler.listSchedules(PROG11_ID));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(PROG2_ID));
    Assert.assertEquals(ImmutableList.of(psched1, tsched11), scheduler.listSchedules(APP1_ID));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(APP2_ID));


    // attempt to add it back together with a schedule that exists
    try {
      scheduler.addSchedules(ImmutableList.of(tsched1, tsched11));
      Assert.fail("expected AlreadyExistsException");
    } catch (AlreadyExistsException e) {
      // expected
    }
    Assert.assertEquals(ImmutableList.of(psched1), scheduler.listSchedules(PROG1_ID));
    Assert.assertEquals(ImmutableList.of(tsched11), scheduler.listSchedules(PROG11_ID));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(PROG2_ID));
    Assert.assertEquals(ImmutableList.of(psched1, tsched11), scheduler.listSchedules(APP1_ID));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(APP2_ID));

    // add it back, delete all schedules for one app
    scheduler.addSchedule(tsched1);
    scheduler.deleteSchedules(APP1_ID);
    verifyNotFound(scheduler, TSCHED1_ID);
    verifyNotFound(scheduler, PSCHED1_ID);
    verifyNotFound(scheduler, TSCHED11_ID);
    Assert.assertEquals(ImmutableList.of(), scheduler.listSchedules(PROG1_ID));
    Assert.assertEquals(ImmutableList.of(), scheduler.listSchedules(PROG11_ID));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(PROG2_ID));
    Assert.assertEquals(ImmutableList.of(), scheduler.listSchedules(APP1_ID));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(PROG2_ID));
  }

  private static void verifyNotFound(Scheduler scheduler, ScheduleId scheduleId) {
    try {
      scheduler.getSchedule(scheduleId);
      Assert.fail("expected NotFoundException");
    } catch (NotFoundException e) {
      // expected
    }
  }

  @Test
  @Category(XSlowTests.class)
  public void testRunScheduledJobs() throws Exception {
    messagingService = getInjector().getInstance(MessagingService.class);
    CConfiguration cConf = getInjector().getInstance(CConfiguration.class);
    dataEventTopic = NamespaceId.SYSTEM.topic(cConf.get(Constants.Dataset.DATA_EVENT_TOPIC));
    store = getInjector().getInstance(Store.class);

    deploy(AppWithFrequentScheduledWorkflows.class);

    // Resume the schedule because schedules are initialized as paused
    enableSchedule(AppWithFrequentScheduledWorkflows.ONE_MIN_SCHEDULE_1);
    enableSchedule(AppWithFrequentScheduledWorkflows.ONE_MIN_SCHEDULE_2);
    enableSchedule(AppWithFrequentScheduledWorkflows.DATASET_PARTITION_SCHEDULE_1);
    enableSchedule(AppWithFrequentScheduledWorkflows.DATASET_PARTITION_SCHEDULE_2);

    long startTime = System.currentTimeMillis();
    for (int i = 0; i < 5; i++) {
      testNewPartition(i + 1);
    }

    // disable the two partition schedules, send them notifications (but they should not trigger)
    int runs1 = getRuns(WORKFLOW_1);
    int runs2 = getRuns(WORKFLOW_2);
    disableSchedule(AppWithFrequentScheduledWorkflows.DATASET_PARTITION_SCHEDULE_1);
    disableSchedule(AppWithFrequentScheduledWorkflows.DATASET_PARTITION_SCHEDULE_2);
    publishNotification(dataEventTopic, WORKFLOW_1, AppWithFrequentScheduledWorkflows.DATASET_NAME1);
    publishNotification(dataEventTopic, WORKFLOW_2, AppWithFrequentScheduledWorkflows.DATASET_NAME2);
    publishNotification(dataEventTopic, WORKFLOW_2, AppWithFrequentScheduledWorkflows.DATASET_NAME2);

    long elapsedTime = System.currentTimeMillis() - startTime;
    // Sleep to wait for one run of WORKFLOW_3 to complete as scheduled
    long sleepTime = TimeUnit.SECONDS.toMillis(75) - elapsedTime;
    if (sleepTime > 0) {
      Thread.sleep(TimeUnit.SECONDS.toMillis(75) - elapsedTime);
    }
    // Both workflows must run at least once.
    // If the testNewPartition() loop took longer than expected, it may be more (quartz fired multiple times)
    Assert.assertTrue(0 < getRuns(SCHEDULED_WORKFLOW_1));
    Assert.assertTrue(0 < getRuns(SCHEDULED_WORKFLOW_2));

    // verify that the two partition schedules did not trigger
    Assert.assertEquals(runs1, getRuns(WORKFLOW_1));
    Assert.assertEquals(runs2, getRuns(WORKFLOW_2));

    // enable partition schedule 2
    enableSchedule(AppWithFrequentScheduledWorkflows.DATASET_PARTITION_SCHEDULE_2);
    testScheduleUpdate("disable");
    testScheduleUpdate("update");
    testScheduleUpdate("delete");
  }

  private void testScheduleUpdate(String howToUpdate) throws Exception {
    int runs = getRuns(WORKFLOW_2);
    ScheduleId scheduleId2 = APP_ID.schedule(AppWithFrequentScheduledWorkflows.DATASET_PARTITION_SCHEDULE_2);
    // send one notification to it, that will not start the workflow
    publishNotification(dataEventTopic, WORKFLOW_2, AppWithFrequentScheduledWorkflows.DATASET_NAME2);
    TimeUnit.SECONDS.sleep(5); // give it enough time to create the job
    Assert.assertEquals(runs, getRuns(WORKFLOW_2));
    if ("disable".equals(howToUpdate)) {
      // disabling and enabling the schedule should remove the job
      disableSchedule(AppWithFrequentScheduledWorkflows.DATASET_PARTITION_SCHEDULE_2);
      enableSchedule(AppWithFrequentScheduledWorkflows.DATASET_PARTITION_SCHEDULE_2);
    } else {
      ProgramSchedule schedule = scheduler.getSchedule(scheduleId2);
      Map<String, String> updatedProperties = ImmutableMap.<String, String>builder()
        .putAll(schedule.getProperties()).put(howToUpdate, howToUpdate).build();
      ProgramSchedule updatedSchedule = new ProgramSchedule(schedule.getName(), schedule.getDescription(),
                                                            schedule.getProgramId(), updatedProperties,
                                                            schedule.getTrigger(), schedule.getConstraints());
      if ("update".equals(howToUpdate)) {
        scheduler.updateSchedule(updatedSchedule);
        Assert.assertEquals(ProgramScheduleStatus.SCHEDULED, scheduler.getScheduleStatus(scheduleId2));
      } else if ("delete".equals(howToUpdate)) {
        scheduler.deleteSchedule(scheduleId2);
        scheduler.addSchedule(updatedSchedule);
        enableSchedule(scheduleId2.getSchedule());
      } else {
        Assert.fail("invalid howToUpdate: " + howToUpdate);
      }
    }
    // single notification should not trigger workflow 2 yet (if it does, then the job was not removed)
    publishNotification(dataEventTopic, WORKFLOW_2, AppWithFrequentScheduledWorkflows.DATASET_NAME2);
    TimeUnit.SECONDS.sleep(10); // give it enough time to create the job, but should not start
    Assert.assertEquals(runs, getRuns(WORKFLOW_2));
    // now this should kick off the workflow
    publishNotification(dataEventTopic, WORKFLOW_2, AppWithFrequentScheduledWorkflows.DATASET_NAME2);
    waitForCompleteRuns(runs + 1, WORKFLOW_2);
  }

  private void enableSchedule(String name) throws NotFoundException, ConflictException {
    ScheduleId scheduleId = APP_ID.schedule(name);
    scheduler.enableSchedule(scheduleId);
    Assert.assertEquals(ProgramScheduleStatus.SCHEDULED, scheduler.getScheduleStatus(scheduleId));
  }

  private void disableSchedule(String name) throws NotFoundException, ConflictException {
    ScheduleId scheduleId = APP_ID.schedule(name);
    scheduler.disableSchedule(scheduleId);
    Assert.assertEquals(ProgramScheduleStatus.SUSPENDED, scheduler.getScheduleStatus(scheduleId));
  }

  private void testNewPartition(int expectedNumRuns) throws Exception {
    publishNotification(dataEventTopic, WORKFLOW_1, AppWithFrequentScheduledWorkflows.DATASET_NAME1);
    publishNotification(dataEventTopic, WORKFLOW_2, AppWithFrequentScheduledWorkflows.DATASET_NAME2);
    publishNotification(dataEventTopic, WORKFLOW_2, AppWithFrequentScheduledWorkflows.DATASET_NAME2);

    try {
      waitForCompleteRuns(expectedNumRuns, WORKFLOW_1);
      waitForCompleteRuns(expectedNumRuns, WORKFLOW_2);
    } finally {
      LOG.info("WORKFLOW_1 runRecords: {}", getRuns(WORKFLOW_1));
      LOG.info("WORKFLOW_2 runRecords: {}", getRuns(WORKFLOW_2));
    }
  }

  private void waitForCompleteRuns(int numRuns, final ProgramId program)
    throws InterruptedException, ExecutionException, TimeoutException {

    Tasks.waitFor(numRuns, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return getRuns(program);
      }
    }, 10, TimeUnit.SECONDS);
  }

  private int getRuns(ProgramId workflowId) {
    return store.getRuns(workflowId, ProgramRunStatus.ALL, 0, Long.MAX_VALUE, Integer.MAX_VALUE).size();
  }

  private void publishNotification(TopicId topicId, ProgramId programId, String dataset)
    throws TopicNotFoundException, IOException, TransactionFailureException,
    AlreadyExistsException, BadRequestException {

    DatasetId datasetId = programId.getNamespaceId().dataset(dataset);
    PartitionKey partitionKey = PartitionKey.builder().addIntField("part1", 1).build();
    Notification notification = Notification.forPartitions(datasetId, ImmutableList.of(partitionKey));
    messagingService.publish(StoreRequestBuilder.of(topicId).addPayloads(GSON.toJson(notification)).build());
  }
}
