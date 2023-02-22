/*
 * Copyright © 2017-2020 Cask Data, Inc.
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

package io.cdap.cdap.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.AppWithFrequentScheduledWorkflows;
import io.cdap.cdap.AppWithMultipleSchedules;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.dataset.lib.PartitionKey;
import io.cdap.cdap.api.schedule.TriggerInfo;
import io.cdap.cdap.api.schedule.TriggeringScheduleInfo;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.ProfileConflictException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramScheduleStatus;
import io.cdap.cdap.internal.app.runtime.schedule.TriggeringScheduleInfoAdapter;
import io.cdap.cdap.internal.app.runtime.schedule.queue.Job;
import io.cdap.cdap.internal.app.runtime.schedule.queue.JobQueueTable;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.PartitionTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.client.StoreRequestBuilder;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.ProtoTrigger;
import io.cdap.cdap.proto.RunRecord;
import io.cdap.cdap.proto.WorkflowTokenDetail;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.codec.WorkflowTokenDetailCodec;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramReference;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.test.XSlowTests;
import io.cdap.common.http.HttpResponse;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

public class CoreSchedulerServiceTest extends AppFabricTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(CoreSchedulerServiceTest.class);

  private static final NamespaceId NS_ID = new NamespaceId("schedtest");
  private static final ApplicationId APP1_ID = NS_ID.app("app1");
  private static final ApplicationId APP2_ID = NS_ID.app("app2");
  private static final ApplicationReference APP1_REF = APP1_ID.getAppReference();
  private static final ApplicationReference APP2_REF = APP2_ID.getAppReference();
  private static final ProgramReference PROG1_REF = APP1_REF.program(ProgramType.WORKFLOW, "wf1");
  private static final ProgramReference PROG2_REF = APP2_REF.program(ProgramType.WORKFLOW, "wf2");
  private static final ProgramReference PROG11_REF = APP1_REF.program(ProgramType.WORKFLOW, "wf11");
  private static final ScheduleId PSCHED1_ID = APP1_ID.schedule("psched1");
  private static final ScheduleId PSCHED2_ID = APP2_ID.schedule("psched2");
  private static final ScheduleId TSCHED1_ID = APP1_ID.schedule("tsched1");
  private static final ScheduleId TSCHED11_ID = APP1_ID.schedule("tsched11");
  private static final DatasetId DS1_ID = NS_ID.dataset("pfs1");
  private static final DatasetId DS2_ID = NS_ID.dataset("pfs2");
  private static ApplicationId appId = NamespaceId.DEFAULT.app("AppWithFrequentScheduledWorkflows", VERSION1);
  private static ApplicationId appMultId = NamespaceId.DEFAULT.app(AppWithMultipleSchedules.NAME);

  private static final int BUFFER = 10;

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private static final Gson GSON = TriggeringScheduleInfoAdapter.addTypeAdapters(new GsonBuilder())
    .registerTypeAdapter(WorkflowTokenDetail.class, new WorkflowTokenDetailCodec())
    .create();

  private static CConfiguration cConf;
  private static MessagingService messagingService;
  private static Store store;
  private static TopicId dataEventTopic;

  private static Scheduler scheduler;
  private static TransactionRunner transactionRunner;

  @BeforeClass
  public static void setup() {
    cConf = getInjector().getInstance(CConfiguration.class);
    scheduler = getInjector().getInstance(Scheduler.class);
    if (scheduler instanceof Service) {
      ((Service) scheduler).startAndWait();
    }
    messagingService = getInjector().getInstance(MessagingService.class);
    store = getInjector().getInstance(Store.class);
    transactionRunner = getInjector().getInstance(TransactionRunner.class);
  }

  @AfterClass
  public static void tearDown() {
    if (scheduler instanceof Service) {
      ((Service) scheduler).stopAndWait();
    }
  }

  @Test
  public void addListDeleteSchedules() throws Exception {
    // verify that list returns nothing
    Assert.assertTrue(scheduler.listSchedules(APP1_REF).isEmpty());
    Assert.assertTrue(scheduler.listSchedules(PROG1_REF).isEmpty());

    // add a schedule for app1
    ProgramSchedule tsched1 = new ProgramSchedule("tsched1", "one time schedule", PROG1_REF,
                                                  ImmutableMap.of("prop1", "nn"),
                                                  new TimeTrigger("* * ? * 1"), ImmutableList.<Constraint>of());
    scheduler.addSchedule(tsched1);
    Assert.assertEquals(tsched1, scheduler.getSchedule(TSCHED1_ID));
    Assert.assertEquals(ImmutableList.of(tsched1), scheduler.listSchedules(APP1_REF));
    Assert.assertEquals(ImmutableList.of(tsched1), scheduler.listSchedules(PROG1_REF));

    // add three more schedules, one for the same program, one for the same app, one for another app
    ProgramSchedule psched1 = new ProgramSchedule("psched1", "one partition schedule", PROG1_REF,
                                                  ImmutableMap.of("prop3", "abc"),
                                                  new PartitionTrigger(DS1_ID, 1), Collections.emptyList());
    ProgramSchedule tsched11 = new ProgramSchedule("tsched11", "two times schedule", PROG11_REF,
                                                   ImmutableMap.of("prop2", "xx"),
                                                   new TimeTrigger("* * ? * 1,2"), Collections.emptyList());
    ProgramSchedule psched2 = new ProgramSchedule("psched2", "two partition schedule", PROG2_REF,
                                                  ImmutableMap.of("propper", "popper"),
                                                  new PartitionTrigger(DS2_ID, 2), Collections.emptyList());
    scheduler.addSchedules(ImmutableList.of(psched1, tsched11, psched2));
    Assert.assertEquals(psched1, scheduler.getSchedule(PSCHED1_ID));
    Assert.assertEquals(tsched11, scheduler.getSchedule(TSCHED11_ID));
    Assert.assertEquals(psched2, scheduler.getSchedule(PSCHED2_ID));

    // list by app and program
    Assert.assertEquals(ImmutableList.of(psched1, tsched1), scheduler.listSchedules(PROG1_REF));
    Assert.assertEquals(ImmutableList.of(tsched11), scheduler.listSchedules(PROG11_REF));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(PROG2_REF));
    Assert.assertEquals(ImmutableList.of(psched1, tsched1, tsched11), scheduler.listSchedules(APP1_REF));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(APP2_REF));

    // delete one schedule
    scheduler.deleteSchedule(TSCHED1_ID);
    verifyNotFound(scheduler, TSCHED1_ID);
    Assert.assertEquals(ImmutableList.of(psched1), scheduler.listSchedules(PROG1_REF));
    Assert.assertEquals(ImmutableList.of(tsched11), scheduler.listSchedules(PROG11_REF));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(PROG2_REF));
    Assert.assertEquals(ImmutableList.of(psched1, tsched11), scheduler.listSchedules(APP1_REF));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(APP2_REF));

    // attempt to delete it again along with another one that exists
    try {
      scheduler.deleteSchedules(ImmutableList.of(TSCHED1_ID, TSCHED11_ID));
      Assert.fail("expected NotFoundException");
    } catch (NotFoundException e) {
      // expected
    }
    Assert.assertEquals(ImmutableList.of(psched1), scheduler.listSchedules(PROG1_REF));
    Assert.assertEquals(ImmutableList.of(tsched11), scheduler.listSchedules(PROG11_REF));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(PROG2_REF));
    Assert.assertEquals(ImmutableList.of(psched1, tsched11), scheduler.listSchedules(APP1_REF));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(APP2_REF));


    // attempt to add it back together with a schedule that exists
    try {
      scheduler.addSchedules(ImmutableList.of(tsched1, tsched11));
      Assert.fail("expected AlreadyExistsException");
    } catch (AlreadyExistsException e) {
      // expected
    }
    Assert.assertEquals(ImmutableList.of(psched1), scheduler.listSchedules(PROG1_REF));
    Assert.assertEquals(ImmutableList.of(tsched11), scheduler.listSchedules(PROG11_REF));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(PROG2_REF));
    Assert.assertEquals(ImmutableList.of(psched1, tsched11), scheduler.listSchedules(APP1_REF));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(APP2_REF));

    // add it back, delete all schedules for one app
    scheduler.addSchedule(tsched1);
    scheduler.deleteSchedules(APP1_REF);
    verifyNotFound(scheduler, TSCHED1_ID);
    verifyNotFound(scheduler, PSCHED1_ID);
    verifyNotFound(scheduler, TSCHED11_ID);
    Assert.assertEquals(ImmutableList.of(), scheduler.listSchedules(PROG1_REF));
    Assert.assertEquals(ImmutableList.of(), scheduler.listSchedules(PROG11_REF));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(PROG2_REF));
    Assert.assertEquals(ImmutableList.of(), scheduler.listSchedules(APP1_REF));
    Assert.assertEquals(ImmutableList.of(psched2), scheduler.listSchedules(PROG2_REF));
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
    CConfiguration cConf = getInjector().getInstance(CConfiguration.class);
    dataEventTopic = NamespaceId.SYSTEM.topic(cConf.get(Constants.Dataset.DATA_EVENT_TOPIC));

    // Deploy the app with version
    Id.Artifact appArtifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "appwithschedules", VERSION1);
    addAppArtifact(appArtifactId, AppWithFrequentScheduledWorkflows.class);
    AppRequest<? extends Config> appRequest = new AppRequest<>(
      new ArtifactSummary(appArtifactId.getName(), appArtifactId.getVersion().getVersion()));
    deploy(appId, appRequest);
    ApplicationDetail appDetails = getAppDetails(appId.getNamespace(), appId.getApplication());
    appId = new ApplicationId(appId.getNamespace(), appId.getApplication(), appDetails.getAppVersion());

    // Resume the schedule because schedules are initialized as paused
    enableSchedule(appId, AppWithFrequentScheduledWorkflows.TEN_SECOND_SCHEDULE_1);
    enableSchedule(appId, AppWithFrequentScheduledWorkflows.TEN_SECOND_SCHEDULE_2);
    enableSchedule(appId, AppWithFrequentScheduledWorkflows.DATASET_PARTITION_SCHEDULE_1);
    enableSchedule(appId, AppWithFrequentScheduledWorkflows.DATASET_PARTITION_SCHEDULE_2);

    ProgramId workflow1 = appId.program(ProgramType.WORKFLOW, AppWithFrequentScheduledWorkflows.SOME_WORKFLOW);
    ProgramId workflow2 = appId.program(ProgramType.WORKFLOW, AppWithFrequentScheduledWorkflows.ANOTHER_WORKFLOW);
    for (int i = 0; i < 5; i++) {
      testNewPartition(workflow1, workflow2, i + 1);
    }

    // Enable COMPOSITE_SCHEDULE before publishing events to DATASET_NAME2
    enableSchedule(appId, AppWithFrequentScheduledWorkflows.COMPOSITE_SCHEDULE);

    // disable the two partition schedules, send them notifications (but they should not trigger)

    ProgramId scheduledWorkflow1 = appId.program(ProgramType.WORKFLOW,
                                                 AppWithFrequentScheduledWorkflows.SCHEDULED_WORKFLOW_1);
    ProgramId scheduledWorkflow2 = appId.program(ProgramType.WORKFLOW,
                                                 AppWithFrequentScheduledWorkflows.SCHEDULED_WORKFLOW_2);
    int runs1 = getRuns(workflow1, ProgramRunStatus.ALL);
    int runs2 = getRuns(workflow2, ProgramRunStatus.ALL);
    disableSchedule(appId, AppWithFrequentScheduledWorkflows.DATASET_PARTITION_SCHEDULE_1);

    // ensure schedule 2 is disabled after schedule 1
    Thread.sleep(BUFFER);
    long disableBeforeTime = System.currentTimeMillis();
    disableSchedule(appId, AppWithFrequentScheduledWorkflows.DATASET_PARTITION_SCHEDULE_2);
    long disableAfterTime = System.currentTimeMillis() + 1;

    publishNotification(dataEventTopic, NamespaceId.DEFAULT, AppWithFrequentScheduledWorkflows.DATASET_NAME1);
    long minPublishTime = System.currentTimeMillis();
    publishNotification(dataEventTopic, NamespaceId.DEFAULT, AppWithFrequentScheduledWorkflows.DATASET_NAME2);
    // This would make sure the subscriber has processed the data event
    waitUntilProcessed(dataEventTopic, minPublishTime);

    // Both workflows must run at least once.
    // If the testNewPartition() loop took longer than expected, it may be more (quartz fired multiple times)
    Tasks.waitFor(true, () -> getRuns(scheduledWorkflow1, ProgramRunStatus.COMPLETED) > 0
          && getRuns(scheduledWorkflow2, ProgramRunStatus.COMPLETED) > 0, 10, TimeUnit.SECONDS);

    // There shouldn't be any partition trigger in the job queue
    Assert.assertFalse(Iterables.any(getAllJobs(), job ->
      job.getSchedule().getTrigger() instanceof ProtoTrigger.PartitionTrigger));

    ProgramId compositeWorkflow = appId.workflow(AppWithFrequentScheduledWorkflows.COMPOSITE_WORKFLOW);
    // Workflow scheduled with the composite trigger has never been started
    Assert.assertEquals(0, getRuns(compositeWorkflow, ProgramRunStatus.ALL));

    // Publish two more new partition notifications to satisfy the partition trigger in the composite trigger,
    // and thus the whole composite trigger will be satisfied
    publishNotification(dataEventTopic, NamespaceId.DEFAULT, AppWithFrequentScheduledWorkflows.DATASET_NAME2);
    minPublishTime = System.currentTimeMillis();
    publishNotification(dataEventTopic, NamespaceId.DEFAULT, AppWithFrequentScheduledWorkflows.DATASET_NAME2);
    // This would make sure the subscriber has processed the data event
    waitUntilProcessed(dataEventTopic, minPublishTime);
    // Wait for 1 run to complete for compositeWorkflow
    waitForCompleteRuns(1, compositeWorkflow);

    for (RunRecordDetail runRecordMeta : store.getRuns(scheduledWorkflow1, ProgramRunStatus.ALL,
                                                       0, Long.MAX_VALUE, Integer.MAX_VALUE).values()) {
      Map<String, String> sysArgs = runRecordMeta.getSystemArgs();
      Assert.assertNotNull(sysArgs);
      TriggeringScheduleInfo scheduleInfo = GSON.fromJson(sysArgs.get(ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO),
                                                  TriggeringScheduleInfo.class);
      Assert.assertEquals(AppWithFrequentScheduledWorkflows.TEN_SECOND_SCHEDULE_1,
                          scheduleInfo.getName());

      List<TriggerInfo> triggerInfos = scheduleInfo.getTriggerInfos();
      // Only one notification is enough to satisfy Time Trigger
      Assert.assertEquals(1, triggerInfos.size());
      Assert.assertEquals(TriggerInfo.Type.TIME, triggerInfos.get(0).getType());
    }

    // Also verify that the two partition schedules did not trigger
    Assert.assertEquals(runs1, getRuns(workflow1, ProgramRunStatus.ALL));
    Assert.assertEquals(runs2, getRuns(workflow2, ProgramRunStatus.ALL));

    // enable partition schedule 2 and test reEnableSchedules
    scheduler.reEnableSchedules(NamespaceId.DEFAULT, disableBeforeTime, disableAfterTime);
    Assert.assertEquals(ProgramScheduleStatus.SCHEDULED, scheduler.getScheduleStatus(
      appId.schedule(AppWithFrequentScheduledWorkflows.DATASET_PARTITION_SCHEDULE_2)));
    Assert.assertEquals(ProgramScheduleStatus.SUSPENDED, scheduler.getScheduleStatus(
      appId.schedule(AppWithFrequentScheduledWorkflows.DATASET_PARTITION_SCHEDULE_1)));
    testScheduleUpdate(workflow2, appId, "disable");
    testScheduleUpdate(workflow2, appId, "update");
    testScheduleUpdate(workflow2, appId, "delete");
  }

  @Test
  @Category(XSlowTests.class)
  public void testProgramEvents() throws Exception {
    // Deploy the app
    deploy(AppWithMultipleSchedules.class, 200);
    ApplicationDetail appDetails = getAppDetails(Id.Namespace.DEFAULT.getId(), AppWithMultipleSchedules.NAME);
    appMultId = new ApplicationId(appMultId.getNamespace(), appMultId.getApplication(), appDetails.getAppVersion());
    ProgramId someWorkflow = appMultId.program(ProgramType.WORKFLOW, AppWithMultipleSchedules.SOME_WORKFLOW);
    ProgramId anotherWorkflow = appMultId.program(ProgramType.WORKFLOW, AppWithMultipleSchedules.ANOTHER_WORKFLOW);
    ProgramId triggeredWorkflow = appMultId.program(ProgramType.WORKFLOW, AppWithMultipleSchedules.TRIGGERED_WORKFLOW);

    CConfiguration cConf = getInjector().getInstance(CConfiguration.class);
    TopicId programEventTopic =
      NamespaceId.SYSTEM.topic(cConf.get(Constants.AppFabric.PROGRAM_STATUS_RECORD_EVENT_TOPIC));
    ProgramStateWriter programStateWriter = getInjector().getInstance(ProgramStateWriter.class);

    // These notifications should not trigger the program
    ProgramRunId anotherWorkflowRun = anotherWorkflow.run(RunIds.generate());

    ArtifactId artifactId = anotherWorkflow.getNamespaceId().artifact("test", "1.0").toApiArtifactId();
    ApplicationSpecification appSpec = new DefaultApplicationSpecification(
      AppWithMultipleSchedules.NAME, appDetails.getAppVersion(), ProjectInfo.getVersion().toString(),
      "desc", null, artifactId,
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
      Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
      Collections.emptyMap());
    ProgramDescriptor programDescriptor = new ProgramDescriptor(anotherWorkflowRun.getParent(), appSpec);
    BasicArguments systemArgs =
      new BasicArguments(ImmutableMap.of(ProgramOptionConstants.SKIP_PROVISIONING, Boolean.TRUE.toString()));
    ProgramOptions programOptions = new SimpleProgramOptions(anotherWorkflowRun.getParent(),
                                                             systemArgs, new BasicArguments(), false);
    programStateWriter.start(anotherWorkflowRun, programOptions, null, programDescriptor);
    programStateWriter.running(anotherWorkflowRun, null);
    long lastProcessed = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    programStateWriter.error(anotherWorkflowRun, null);
    waitUntilProcessed(programEventTopic, lastProcessed);

    ProgramRunId someWorkflowRun = someWorkflow.run(RunIds.generate());
    programDescriptor = new ProgramDescriptor(someWorkflowRun.getParent(), appSpec);
    programStateWriter.start(someWorkflowRun, new SimpleProgramOptions(someWorkflowRun.getParent(),
                                                                       systemArgs, new BasicArguments()),
                             null, programDescriptor);
    programStateWriter.running(someWorkflowRun, null);
    lastProcessed = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    programStateWriter.killed(someWorkflowRun);
    waitUntilProcessed(programEventTopic, lastProcessed);
    Assert.assertEquals(0, getRuns(triggeredWorkflow, ProgramRunStatus.ALL));

    // Enable the schedule
    ApplicationId appId = new ApplicationId(appMultId.getNamespace(), appMultId.getApplication(),
                                            appDetails.getAppVersion());
    scheduler.enableSchedule(appId.schedule(AppWithMultipleSchedules.WORKFLOW_COMPLETED_SCHEDULE));

    // Start a program with user arguments
    startProgram(anotherWorkflow, ImmutableMap.of(AppWithMultipleSchedules.ANOTHER_RUNTIME_ARG_KEY,
                                                   AppWithMultipleSchedules.ANOTHER_RUNTIME_ARG_VALUE), 200);

    // Wait for a completed run record
    waitForCompleteRuns(1, triggeredWorkflow);
    assertProgramRuns(triggeredWorkflow, ProgramRunStatus.COMPLETED, 1);
    RunRecord run = getProgramRuns(triggeredWorkflow, ProgramRunStatus.COMPLETED).get(0);
    Map<String, List<WorkflowTokenDetail.NodeValueDetail>> tokenData =
      getWorkflowToken(triggeredWorkflow, run.getPid(), null, null).getTokenData();
    // There should be 2 entries in tokenData
    Assert.assertEquals(2, tokenData.size());
    // The value of TRIGGERED_RUNTIME_ARG_KEY should be ANOTHER_RUNTIME_ARG_VALUE from the triggering workflow
    Assert.assertEquals(AppWithMultipleSchedules.ANOTHER_RUNTIME_ARG_VALUE,
                        tokenData.get(AppWithMultipleSchedules.TRIGGERED_RUNTIME_ARG_KEY).get(0).getValue());
    // The value of TRIGGERED_TOKEN_KEY should be ANOTHER_TOKEN_VALUE from the triggering workflow
    Assert.assertEquals(AppWithMultipleSchedules.ANOTHER_TOKEN_VALUE,
                        tokenData.get(AppWithMultipleSchedules.TRIGGERED_TOKEN_KEY).get(0).getValue());
  }

  @Test
  public void testAddScheduleWithDisabledProfile() throws Exception {
    // put my profile and by default it is enabled
    ProfileId profileId = NS_ID.profile("MyProfile");
    Profile profile = new Profile("MyProfile", Profile.NATIVE.getLabel(), Profile.NATIVE.getDescription(),
                                  Profile.NATIVE.getScope(), Profile.NATIVE.getProvisioner());
    putProfile(profileId, profile, 200);

    // add a schedule, it should succeed since the profile is enabled.
    ProgramSchedule tsched1 =
      new ProgramSchedule("tsched1", "one time schedule", PROG1_REF,
                          ImmutableMap.of("prop1", "nn", SystemArguments.PROFILE_NAME, "USER:MyProfile"),
                          new TimeTrigger("* * ? * 1"), ImmutableList.<Constraint>of());
    scheduler.addSchedule(tsched1);
    Assert.assertEquals(Collections.singletonList(tsched1), scheduler.listSchedules(PROG1_REF));

    // now disable the profile
    disableProfile(profileId, 200);

    // delete it
    scheduler.deleteSchedule(TSCHED1_ID);
    Assert.assertEquals(Collections.emptyList(), scheduler.listSchedules(PROG1_REF));

    // add it again should also fail since the profile is disabled
    try {
      scheduler.addSchedule(tsched1);
      Assert.fail();
    } catch (ProfileConflictException e) {
      // expected
    }

    // clean up
    deleteProfile(profileId, 200);
  }

  @Test
  public void testAddDeleteScheduleWithProfileProperty() throws Exception {
    // put my profile and by default it is enabled
    ProfileId profileId = NS_ID.profile("MyProfile");
    putProfile(profileId, Profile.NATIVE, 200);

    // add a schedule, it should succeed since the profile is enabled.
    ProgramSchedule tsched1 =
      new ProgramSchedule("tsched1", "one time schedule", PROG1_REF,
                          ImmutableMap.of("prop1", "nn", SystemArguments.PROFILE_NAME, profileId.getScopedName()),
                          new TimeTrigger("* * ? * 1"), ImmutableList.<Constraint>of());
    scheduler.addSchedule(tsched1);

    // now disable the profile and delete, deletion should fail because the profile is now associated with the schedule
    disableProfile(profileId, 200);
    deleteProfile(profileId, 409);

    // delete it
    scheduler.deleteSchedule(TSCHED1_ID);
    // now deletion should succeed since it should remove assignment from the profile
    deleteProfile(profileId, 200);
  }

  private WorkflowTokenDetail getWorkflowToken(ProgramId workflowId, String runId,
                                               @Nullable WorkflowToken.Scope scope,
                                               @Nullable String key) throws Exception {
    String workflowTokenUrl = String.format("apps/%s/workflows/%s/runs/%s/token", workflowId.getApplication(),
                                            workflowId.getProgram(), runId);
    String versionedUrl = getVersionedAPIPath(appendScopeAndKeyToUrl(workflowTokenUrl, scope, key),
                                              Constants.Gateway.API_VERSION_3_TOKEN, workflowId.getNamespace());
    HttpResponse response = doGet(versionedUrl);
    return readResponse(response, new TypeToken<WorkflowTokenDetail>() { }.getType(), GSON);
  }

  private String appendScopeAndKeyToUrl(String workflowTokenUrl, @Nullable WorkflowToken.Scope scope, String key) {
    StringBuilder output = new StringBuilder(workflowTokenUrl);
    if (scope != null) {
      output.append(String.format("?scope=%s", scope.name()));
      if (key != null) {
        output.append(String.format("&key=%s", key));
      }
    } else if (key != null) {
      output.append(String.format("?key=%s", key));
    }
    return output.toString();
  }

  private void testScheduleUpdate(ProgramId workflow2, ApplicationId appId, String howToUpdate) throws Exception {
    int runs = getRuns(workflow2, ProgramRunStatus.ALL);
    final ScheduleId scheduleId2 = appId.schedule(AppWithFrequentScheduledWorkflows.DATASET_PARTITION_SCHEDULE_2);

    // send one notification to it
    long minPublishTime = System.currentTimeMillis();
    publishNotification(dataEventTopic, NamespaceId.DEFAULT, AppWithFrequentScheduledWorkflows.DATASET_NAME2);
    waitUntilProcessed(dataEventTopic, minPublishTime);

    // A pending job will be created, but it won't run
    Assert.assertTrue("Expected a PENDING_TRIGGER job for " + scheduleId2,
                      Iterables.any(getAllJobs(), job -> {
                        if (!(job.getSchedule().getTrigger() instanceof ProtoTrigger.PartitionTrigger)) {
                          return false;
                        }
                        return scheduleId2.equals(job.getJobKey().getScheduleId()) &&
                          job.getState() == Job.State.PENDING_TRIGGER;

                      }));

    Assert.assertEquals(runs, getRuns(workflow2, ProgramRunStatus.ALL));

    if ("disable".equals(howToUpdate)) {
      // disabling and enabling the schedule should remove the job
      disableSchedule(appId, AppWithFrequentScheduledWorkflows.DATASET_PARTITION_SCHEDULE_2);
      enableSchedule(appId, AppWithFrequentScheduledWorkflows.DATASET_PARTITION_SCHEDULE_2);
    } else {
      ProgramSchedule schedule = scheduler.getSchedule(scheduleId2);
      Map<String, String> updatedProperties = ImmutableMap.<String, String>builder()
        .putAll(schedule.getProperties()).put(howToUpdate, howToUpdate).build();
      ProgramSchedule updatedSchedule = new ProgramSchedule(schedule.getName(), schedule.getDescription(),
                                                            schedule.getProgramReference(), updatedProperties,
                                                            schedule.getTrigger(), schedule.getConstraints());
      if ("update".equals(howToUpdate)) {
        scheduler.updateSchedule(updatedSchedule);
        Assert.assertEquals(ProgramScheduleStatus.SCHEDULED, scheduler.getScheduleStatus(scheduleId2));
      } else if ("delete".equals(howToUpdate)) {
        scheduler.deleteSchedule(scheduleId2);
        scheduler.addSchedule(updatedSchedule);
        enableSchedule(appId, scheduleId2.getSchedule());
      } else {
        Assert.fail("invalid howToUpdate: " + howToUpdate);
      }
    }
    // single notification should not trigger workflow 2 yet (if it does, then the job was not removed)
    minPublishTime = System.currentTimeMillis();
    publishNotification(dataEventTopic, NamespaceId.DEFAULT, AppWithFrequentScheduledWorkflows.DATASET_NAME2);
    waitUntilProcessed(dataEventTopic, minPublishTime);

    // Again, a pending job will be created, but it won't run since updating the schedule would remove pending trigger
    Assert.assertTrue("Expected a PENDING_TRIGGER job for " + scheduleId2,
                      Iterables.any(getAllJobs(), job -> {
                        if (!(job.getSchedule().getTrigger() instanceof ProtoTrigger.PartitionTrigger)) {
                          return false;
                        }
                        return scheduleId2.equals(job.getJobKey().getScheduleId()) &&
                          job.getState() == Job.State.PENDING_TRIGGER;
                      }));

    Assert.assertEquals(runs, getRuns(workflow2, ProgramRunStatus.ALL));
    // publish one more notification, this should kick off the workflow
    publishNotification(dataEventTopic, NamespaceId.DEFAULT, AppWithFrequentScheduledWorkflows.DATASET_NAME2);
    waitForCompleteRuns(runs + 1, workflow2);
  }

  private void enableSchedule(ApplicationId appId, String name) throws NotFoundException, ConflictException {
    ScheduleId scheduleId = appId.schedule(name);
    scheduler.enableSchedule(scheduleId);
    Assert.assertEquals(ProgramScheduleStatus.SCHEDULED, scheduler.getScheduleStatus(scheduleId));
  }

  private void disableSchedule(ApplicationId appId, String name) throws NotFoundException, ConflictException {
    ScheduleId scheduleId = appId.schedule(name);
    scheduler.disableSchedule(scheduleId);
    Assert.assertEquals(ProgramScheduleStatus.SUSPENDED, scheduler.getScheduleStatus(scheduleId));
  }

  private void testNewPartition(ProgramId workflow1, ProgramId workflow2, int expectedNumRuns) throws Exception {
    publishNotification(dataEventTopic, NamespaceId.DEFAULT, AppWithFrequentScheduledWorkflows.DATASET_NAME1);
    publishNotification(dataEventTopic, NamespaceId.DEFAULT, AppWithFrequentScheduledWorkflows.DATASET_NAME2);
    publishNotification(dataEventTopic, NamespaceId.DEFAULT, AppWithFrequentScheduledWorkflows.DATASET_NAME2);

    waitForCompleteRuns(expectedNumRuns, workflow1);
    waitForCompleteRuns(expectedNumRuns, workflow2);
  }

  private void waitForCompleteRuns(int numRuns, final ProgramId program) throws Exception {
    try {
      Tasks.waitFor(numRuns, () -> getRuns(program, ProgramRunStatus.COMPLETED), 30, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.info("waitForCompleteRuns raised an exception, {} runs expected for program {}", numRuns, program);
      store.getRuns(program, ProgramRunStatus.ALL, 0, Long.MAX_VALUE, Integer.MAX_VALUE)
        .forEach((key, value) -> LOG.info("ProgramRunID: {}, RunRecordDetail: {}", key, value));
      throw e;
    }
  }

  private int getRuns(ProgramId workflowId, ProgramRunStatus status) {
    return store.getRuns(workflowId, status, 0, Long.MAX_VALUE, Integer.MAX_VALUE).size();
  }

  private void publishNotification(TopicId topicId, NamespaceId namespaceId, String dataset) throws Exception {
    DatasetId datasetId = namespaceId.dataset(dataset);
    PartitionKey partitionKey = PartitionKey.builder().addIntField("part1", 1).build();
    Notification notification = Notification.forPartitions(datasetId, ImmutableList.of(partitionKey));
    messagingService.publish(StoreRequestBuilder.of(topicId).addPayload(GSON.toJson(notification)).build());
  }

  @Nullable
  private MessageId getLastMessageId(final TopicId topic) {
    return TransactionRunners.run(transactionRunner, context -> {
      JobQueueTable jobQueue = JobQueueTable.getJobQueue(context, cConf);
      String id = jobQueue.retrieveSubscriberState(topic.getTopic());
      if (id == null) {
        return null;
      }
      byte[] bytes = Bytes.fromHexString(id);
      return new MessageId(bytes);
    });
  }

  /**
   * Wait until the scheduler process a message published on or after the given time.
   */
  private void waitUntilProcessed(final TopicId topic, final long minPublishTime) throws Exception {
    // Wait for the persisted message changed. That means the scheduler actually consumed the last data event
    Tasks.waitFor(true, () -> {
      MessageId messageId = getLastMessageId(topic);
      return messageId != null && messageId.getPublishTimestamp() >= minPublishTime;
    }, 5, TimeUnit.SECONDS);
  }

  private List<Job> getAllJobs() {
    return TransactionRunners.run(transactionRunner, context -> {
      JobQueueTable jobQueue = JobQueueTable.getJobQueue(context, cConf);
      try (CloseableIterator<Job> iterator = jobQueue.fullScan()) {
        return Lists.newArrayList(iterator);
      }
    });
  }
}
