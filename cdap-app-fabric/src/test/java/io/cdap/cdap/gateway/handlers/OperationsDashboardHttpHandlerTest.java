/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Injector;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.schedule.TriggerInfo;
import io.cdap.cdap.api.schedule.TriggeringScheduleInfo;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.ConflictException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.schedule.DefaultTriggeringScheduleInfo;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.TriggeringScheduleInfoAdapter;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.DefaultTimeTriggerInfo;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.WorkflowId;
import io.cdap.cdap.proto.ops.DashboardProgramRunRecord;
import io.cdap.cdap.reporting.ProgramHeartbeatTable;
import io.cdap.cdap.scheduler.Scheduler;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.common.http.HttpResponse;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link OperationsDashboardHttpHandler}
 */
public class OperationsDashboardHttpHandlerTest extends AppFabricTestBase {

  private static final ArtifactId ARTIFACT_ID = NamespaceId.DEFAULT.artifact("testArtifact", "1.0");
  private static final ArtifactId ARTIFACT_ID1 = new NamespaceId(TEST_NAMESPACE1).artifact("testArtifact1", "1.0");
  private static final ArtifactId ARTIFACT_ID2 = new NamespaceId(TEST_NAMESPACE2).artifact("testArtifact2", "1.0");
  private static final Gson GSON =
    TriggeringScheduleInfoAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static final ApplicationId APP1_ID = new ApplicationId(TEST_NAMESPACE1, "app1");
  private static final ApplicationId APP2_ID = new ApplicationId(TEST_NAMESPACE2, "app2");
  private static final WorkflowId SCHEDULED_PROG1_ID = APP1_ID.workflow("schedWf1");
  private static final WorkflowId SCHEDULED_PROG2_ID = APP2_ID.workflow("schedWf2");
  private static final String BASE_PATH = Constants.Gateway.API_VERSION_3;
  private static final Type DASHBOARD_DETAIL_TYPE = new TypeToken<List<DashboardProgramRunRecord>>() { }.getType();
  private static Store store;
  private static Scheduler scheduler;
  private static Impersonator impersonator;
  private static final byte[] SOURCE_ID = Bytes.toBytes("sourceId");

  private static TransactionRunner transactionRunner;

  @BeforeClass
  public static void setup() {
    Injector injector = getInjector();
    scheduler = getInjector().getInstance(Scheduler.class);
    impersonator = injector.getInstance(Impersonator.class);
    store = getInjector().getInstance(DefaultStore.class);
    transactionRunner = injector.getInstance(TransactionRunner.class);
  }

  /**
   * writes heart beat messages starting from startTime + interval up to endTime, each heartbeat separated by interval
   */
  private void setUpProgramHeartBeats(RunRecordDetail runRecordMeta,
                                      long startTime, long endTime, long interval) {
    for (long time = startTime + interval; time < endTime; time += interval) {
      writeRunRecordMeta(runRecordMeta, time);
    }
  }

  private void writeRunRecordMeta(RunRecordDetail runRecordMeta,
                                  long timestampInMillis) {
    TransactionRunners.run(transactionRunner, context -> {
      new ProgramHeartbeatTable(context).writeRunRecordMeta(runRecordMeta, timestampInMillis);
    });
  }

  /**
   * setup and return mock program properties on runrecord builder but use passed namespaceId and runId
   */
  private RunRecordDetail.Builder getMockRunRecordMeta(NamespaceId namespaceId, RunId runId) {
    ProgramId programId = namespaceId.app("someapp").program(ProgramType.SERVICE, "s");
    RunRecordDetail.Builder runRecordBuilder = RunRecordDetail.builder();
    runRecordBuilder.setArtifactId(ARTIFACT_ID.toApiArtifactId());
    runRecordBuilder.setPrincipal("userA");
    runRecordBuilder.setProgramRunId(programId.run(runId));
    runRecordBuilder.setSourceId(SOURCE_ID);
    runRecordBuilder.setStartTime(RunIds.getTime(runId, TimeUnit.SECONDS));
    return runRecordBuilder;
  }

  @Test
  public void testDashboardWithNamespaceFiltering() throws Exception {
    long startTime1 = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    RunId runId1 = RunIds.generate();
    NamespaceId ns1 = new NamespaceId("ns1");
    RunRecordDetail.Builder metaBuilder = getMockRunRecordMeta(ns1, runId1);
    metaBuilder.setRunTime(startTime1);
    metaBuilder.setStatus(ProgramRunStatus.RUNNING);
    RunRecordDetail meta = metaBuilder.build();
    writeRunRecordMeta(meta, startTime1);

    // write heart beat messages for 10 minutes (every minute) for this program run.
    long endTime = startTime1 + TimeUnit.MINUTES.toSeconds(10);
    long interval = TimeUnit.MINUTES.toSeconds(1);
    setUpProgramHeartBeats(meta, startTime1, endTime, interval);

    // write end notification message
    metaBuilder.setStopTime(endTime);
    metaBuilder.setStatus(ProgramRunStatus.COMPLETED);
    meta = metaBuilder.build();
    writeRunRecordMeta(meta, endTime);

    long startTime2 = startTime1 + TimeUnit.MINUTES.toSeconds(5);
    NamespaceId ns2 = new NamespaceId("ns2");
    RunId runId2 = RunIds.generate();
    RunRecordDetail.Builder metaBuilder2 = getMockRunRecordMeta(ns2, runId2);
    metaBuilder2.setRunTime(startTime2);
    metaBuilder2.setStatus(ProgramRunStatus.RUNNING);
    RunRecordDetail meta2 = metaBuilder2.build();
    writeRunRecordMeta(meta2, startTime2);

    // write heart beat messages for 5 minutes (every minute) for this program run.
    // this program doesnt have endTime for this testOp
    setUpProgramHeartBeats(meta2, startTime2, endTime, interval);

    String opsDashboardQueryPath =
      String.format("%s/dashboard?start=%s&duration=%s&namespace=%s&namespace=%s", BASE_PATH,
                    String.valueOf(startTime1), String.valueOf(endTime), ns1.getNamespace(), ns2.getNamespace());
    // get ops dashboard query results
    HttpResponse response = doGet(opsDashboardQueryPath);
    Assert.assertEquals(200, response.getResponseCode());
    String content = response.getResponseBodyAsString();
    List<DashboardProgramRunRecord> dashboardDetail = GSON.fromJson(content, DASHBOARD_DETAIL_TYPE);
    // assert the result contains 2 entries
    Assert.assertEquals(2, dashboardDetail.size());
    Set<DashboardProgramRunRecord> expected =
      ImmutableSet.of(OperationsDashboardHttpHandler.runRecordToDashboardRecord(meta),
                      OperationsDashboardHttpHandler.runRecordToDashboardRecord(meta2));
    Assert.assertEquals(expected.size(), dashboardDetail.size());

    // for the same time range query only in namespace ns1 to ensure filtering works fine
    opsDashboardQueryPath =
      String.format("%s/dashboard?start=%s&duration=%s&namespace=%s", BASE_PATH,
                    String.valueOf(startTime1), String.valueOf(endTime), ns2.getNamespace());

    // get ops dashboard query results
    response = doGet(opsDashboardQueryPath);
    Assert.assertEquals(200, response.getResponseCode());
    content = response.getResponseBodyAsString();
    dashboardDetail = GSON.fromJson(content, DASHBOARD_DETAIL_TYPE);
    // assert the result contains 1 entry
    Assert.assertEquals(1, dashboardDetail.size());
    Assert.assertEquals(OperationsDashboardHttpHandler.runRecordToDashboardRecord(meta2),
                        dashboardDetail.iterator().next());

  }

  private List<Long> getExpectedRuntimes(long startTimeInMin, long endTimeInMin,
                                         long triggerTimeInMin, long testStartTime) {
    List<Long> triggerringTimeInSeconds = new ArrayList<>();
    for (long currentTimeInMin = startTimeInMin; currentTimeInMin <= endTimeInMin; currentTimeInMin += 1) {
      if (currentTimeInMin % triggerTimeInMin == 0) {
        // triggering minute
        long timestampInSeconds = TimeUnit.MINUTES.toSeconds(currentTimeInMin);
        if (timestampInSeconds >= testStartTime) {
          // for the first triggering minute its possible it is before the test schedule start time,
          // we need to filter them, example current startTimeInMin is 4:00:00 while actualScheduleStartTime is 4:00:05
          // we need to skip the run time at 4:00:00
          triggerringTimeInSeconds.add(timestampInSeconds);
        }
      }
    }
    return triggerringTimeInSeconds;
  }

  @Test
  public void testScheduledRuns() throws Exception {
    // add app specs for APP1_ID and APP2_ID
    addAppSpecs();
    // add a schedule to be triggered every 30 minutes for SCHEDULED_PROG1_ID
    int sched1Mins = 30;
    ProgramSchedule sched1 = initializeSchedules(sched1Mins, SCHEDULED_PROG1_ID);
    long durationSecs = TimeUnit.HOURS.toSeconds(1);
    // start 1 hr from current time
    long startTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) + TimeUnit.HOURS.toSeconds(1);
    // get ops dashboard results between current time and current time + 3600 from TEST_NAMESPACE1 and TEST_NAMESPACE2
    long endTime = startTime + durationSecs;

    String opsDashboardQueryPath =
      String.format("%s/dashboard?start=%d&duration=%d&namespace=%s&namespace=%s",
                    BASE_PATH, startTime, durationSecs, TEST_NAMESPACE1, TEST_NAMESPACE2);
    List<DashboardProgramRunRecord> dashboardRecords = getDashboardRecords(opsDashboardQueryPath);
    List<Long> runTimesSchedule1 =
      getExpectedRuntimes(TimeUnit.SECONDS.toMinutes(startTime),
                          TimeUnit.SECONDS.toMinutes(endTime), sched1Mins, startTime);

    List<DashboardProgramRunRecord> expectedRunRecords = new ArrayList<>();
    String userId = impersonator.getUGI(sched1.getProgramId()).getUserName();
    for (long runTime : runTimesSchedule1) {
      expectedRunRecords.add(
        new DashboardProgramRunRecord(sched1.getProgramId().getNamespace(),
                                      ArtifactSummary.from(ARTIFACT_ID1.toApiArtifactId()),
                                      new DashboardProgramRunRecord.ApplicationNameVersion(
                                        sched1.getProgramId().getApplication(), sched1.getProgramId().getVersion()),
                                      sched1.getProgramId().getType().name(), sched1.getProgramId().getProgram(),
                                      null, userId, "SCHEDULED", runTime, null, null, null, null, null));
    }
    // assert the number of scheduled runs are expected for both programs
    Assert.assertEquals(expectedRunRecords, dashboardRecords);

    // get ops dashboard results between current time - 7200 and current time - 3600
    // from TEST_NAMESPACE1 and TEST_NAMESPACE2
    String beforeCurrentTimeQueryPath =
      String.format("%s/dashboard?start=%d&duration=%d&namespace=%s&namespace=%s",
                    BASE_PATH, startTime - 2 * durationSecs, durationSecs, TEST_NAMESPACE1, TEST_NAMESPACE2);
    // assert that there's no scheduled runs returned when the end of query time range is before current time
    Assert.assertEquals(0, getDashboardRecords(beforeCurrentTimeQueryPath).size());

    // test with overlap between previous time and time with schedules
    long startTime2 = startTime - durationSecs;
    long duration =  durationSecs * 2;
    opsDashboardQueryPath =
      String.format("%s/dashboard?start=%d&duration=%d&namespace=%s&namespace=%s",
                    BASE_PATH, startTime2, duration, TEST_NAMESPACE1, TEST_NAMESPACE2);
    // assert that there's no scheduled runs returned when the end of query time range is before current time
    Assert.assertEquals(expectedRunRecords, dashboardRecords);

    // disable the schedules
    scheduler.disableSchedule(sched1.getScheduleId());
    // assert that there's no scheduled runs once the schedules are disabled
    Assert.assertEquals(0, getDashboardRecords(opsDashboardQueryPath).size());
  }

  /**
   * Adds {@link ApplicationSpecification} for APP1_ID and APP2_ID.
   */
  private void addAppSpecs() {
    WorkflowSpecification scheduledWorfklow1 =
      new WorkflowSpecification("DummyClass", SCHEDULED_PROG1_ID.getProgram(), "scheduled workflow",
                                Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap(),
                                Collections.emptyMap());
    ApplicationSpecification dummyAppSpec1 =
      new DefaultApplicationSpecification(APP1_ID.getApplication(), "dummy app", null,
                                          ARTIFACT_ID1.toApiArtifactId(),
                                          Collections.emptyMap(), Collections.emptyMap(),
                                          Collections.emptyMap(), Collections.emptyMap(),
                                          ImmutableMap.of(SCHEDULED_PROG1_ID.getProgram(), scheduledWorfklow1),
                                          Collections.emptyMap(), Collections.emptyMap(),
                                          Collections.emptyMap(), Collections.emptyMap());

    store.addApplication(APP1_ID, dummyAppSpec1);
    WorkflowSpecification scheduledWorfklow2 =
      new WorkflowSpecification("DummyClass", SCHEDULED_PROG2_ID.getProgram(), "scheduled workflow",
                                Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap(),
                                Collections.emptyMap());
    ApplicationSpecification dummyAppSpec2 =
      new DefaultApplicationSpecification(APP2_ID.getApplication(), "dummy app", null,
                                          ARTIFACT_ID2.toApiArtifactId(),
                                          Collections.emptyMap(), Collections.emptyMap(),
                                          Collections.emptyMap(), Collections.emptyMap(),
                                          ImmutableMap.of(SCHEDULED_PROG2_ID.getProgram(), scheduledWorfklow2),
                                          Collections.emptyMap(), Collections.emptyMap(),
                                          Collections.emptyMap(), Collections.emptyMap());
    store.addApplication(APP2_ID, dummyAppSpec2);
  }

  /**
   * Adds and enables time based schedules for the given workflow at the given frequency.
   *
   * @param scheduleMins the number of minutes to wait before launching the given workflow each time
   * @param workflowId the ID of the scheduled workflow
   */
  private ProgramSchedule initializeSchedules(int scheduleMins, WorkflowId workflowId)
    throws ConflictException, BadRequestException, NotFoundException {
    ProgramSchedule schedule =
      new ProgramSchedule(String.format("%dMinSchedule", scheduleMins), "time schedule", workflowId,
                          Collections.emptyMap(), new TimeTrigger(String.format("*/%d * * * *", scheduleMins)),
                          Collections.emptyList());
    scheduler.addSchedule(schedule);
    scheduler.enableSchedule(schedule.getScheduleId());
    return schedule;
  }

  private static List<DashboardProgramRunRecord> getDashboardRecords(String path) throws Exception {
    HttpResponse response = doGet(path);
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), DASHBOARD_DETAIL_TYPE);
  }

  @Test
  public void testDashboardReadWithScheduledRuns() throws Exception {
    long startTime1 = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    RunId runId1 = RunIds.generate();
    NamespaceId ns3 = new NamespaceId("ns3");
    List<TriggerInfo> triggerInfos = new ArrayList<>();
    triggerInfos.add(new DefaultTimeTriggerInfo("*/5 * * * *", startTime1));
    TriggeringScheduleInfo triggeringScheduleInfo =
      new DefaultTriggeringScheduleInfo("test", "test", triggerInfos, new HashMap<>());
    RunRecordDetail.Builder metaBuilder = getMockRunRecordMeta(ns3, runId1);
    metaBuilder.setRunTime(startTime1);
    metaBuilder.setStatus(ProgramRunStatus.RUNNING);
    Map<String, String> systemArgs =
      ImmutableMap.of(ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO, GSON.toJson(triggeringScheduleInfo));
    metaBuilder.setSystemArgs(systemArgs);
    RunRecordDetail meta = metaBuilder.build();
    writeRunRecordMeta(meta, startTime1);

    // write heart beat messages for 10 minutes (every minute) for this program run.
    long endTime = startTime1 + TimeUnit.MINUTES.toSeconds(5);
    long interval = TimeUnit.MINUTES.toSeconds(1);
    setUpProgramHeartBeats(meta, startTime1, endTime, interval);

    String opsDashboardQueryPath =
      String.format("%s/dashboard?start=%s&duration=%s&namespace=%s", BASE_PATH,
                    String.valueOf(startTime1), String.valueOf(endTime), ns3.getNamespace());

    // get ops dashboard query results
    HttpResponse response = doGet(opsDashboardQueryPath);
    Assert.assertEquals(200, response.getResponseCode());
    String content = response.getResponseBodyAsString();
    List<DashboardProgramRunRecord> dashboardDetail = GSON.fromJson(content, DASHBOARD_DETAIL_TYPE);
    // assert the result contains 1 entry
    Assert.assertEquals(1, dashboardDetail.size());
    Assert.assertEquals(OperationsDashboardHttpHandler.runRecordToDashboardRecord(meta),
                        dashboardDetail.iterator().next());
  }
}
