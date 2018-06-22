/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.DefaultApplicationSpecification;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.*;
import co.cask.cdap.proto.ops.DashboardProgramRunRecord;
import co.cask.cdap.scheduler.Scheduler;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Tests for {@link OperationsDashboardHttpHandler}
 */
public class OperationsDashboardHttpHandlerTest extends AppFabricTestBase {
  private static final Gson GSON = new Gson();
  private static final ArtifactId ARTIFACT1_ID =
    new ArtifactId(TEST_NAMESPACE1, new ArtifactVersion("1.0.0"), ArtifactScope.USER);
  private static final ArtifactId ARTIFACT2_ID =
    new ArtifactId(TEST_NAMESPACE1, new ArtifactVersion("1.0.0"), ArtifactScope.USER);
  private static final ApplicationId APP1_ID = new ApplicationId(TEST_NAMESPACE1, "app1");
  private static final ApplicationId APP2_ID = new ApplicationId(TEST_NAMESPACE2, "app2");
  private static final WorkflowId SCHEDULED_PROG1_ID = APP1_ID.workflow("schedWf1");
  private static final WorkflowId SCHEDULED_PROG2_ID = APP2_ID.workflow("schedWf2");
  private static final WorkflowId PROG1_ID = APP1_ID.workflow("wf1");
  private static final WorkflowId PROG2_ID = APP2_ID.workflow("wf2");
  private static final WorkflowId PROG3_ID = new WorkflowId(NONEXISTENT_NAMESPACE, "app1", "wf3");
  private static final String BASE_PATH = Constants.Gateway.API_VERSION_3;
  private static final Type DASHBOARD_DETAIL_TYPE = new TypeToken<List<DashboardProgramRunRecord>>() { }.getType();
  private static Store store;
  private static long sourceId;
  private static Scheduler scheduler;

  @BeforeClass
  public static void beforeClass() throws Throwable {
    AppFabricTestBase.beforeClass();
    scheduler = getInjector().getInstance(Scheduler.class);
    if (scheduler instanceof Service) {
      ((Service) scheduler).startAndWait();
    }
    store = getInjector().getInstance(DefaultStore.class);
  }

  @Test
  public void testScheduledRuns() throws Exception {
    WorkflowSpecification scheduledWorfklow1 =
      new WorkflowSpecification("DummyClass", SCHEDULED_PROG1_ID.getProgram(), "scheduled workflow",
        Collections.EMPTY_MAP,Collections.EMPTY_LIST, Collections.EMPTY_MAP);
    ApplicationSpecification dummyAppSpec1 =
      new DefaultApplicationSpecification(APP1_ID.getApplication(), "dummy app", null,
        ARTIFACT1_ID, Collections.EMPTY_MAP, Collections.EMPTY_MAP, Collections.EMPTY_MAP,
        Collections.EMPTY_MAP, Collections.EMPTY_MAP, Collections.EMPTY_MAP,
        ImmutableMap.of(SCHEDULED_PROG1_ID.getProgram(), scheduledWorfklow1), Collections.EMPTY_MAP,
        Collections.EMPTY_MAP, Collections.EMPTY_MAP, Collections.EMPTY_MAP);
    store.addApplication(APP1_ID, dummyAppSpec1);
    WorkflowSpecification scheduledWorfklow2 =
      new WorkflowSpecification("DummyClass", SCHEDULED_PROG2_ID.getProgram(), "scheduled workflow",
        Collections.EMPTY_MAP,Collections.EMPTY_LIST, Collections.EMPTY_MAP);
    ApplicationSpecification dummyAppSpec2 =
      new DefaultApplicationSpecification(APP2_ID.getApplication(), "dummy app", null,
        ARTIFACT2_ID, Collections.EMPTY_MAP, Collections.EMPTY_MAP, Collections.EMPTY_MAP,
        Collections.EMPTY_MAP, Collections.EMPTY_MAP, Collections.EMPTY_MAP,
        ImmutableMap.of(SCHEDULED_PROG2_ID.getProgram(), scheduledWorfklow2), Collections.EMPTY_MAP,
        Collections.EMPTY_MAP, Collections.EMPTY_MAP, Collections.EMPTY_MAP);
    store.addApplication(APP2_ID, dummyAppSpec2);
    // add a schedule to be triggered every 5 minutes for SCHEDULED_PROG1_ID
    int sched1Mins = 5;
    ProgramSchedule sched = new ProgramSchedule("tsched1", "five min schedule", SCHEDULED_PROG1_ID,
      Collections.EMPTY_MAP, new TimeTrigger(String.format("*/%d * * * *", sched1Mins)), Collections.emptyList());
    scheduler.addSchedule(sched);
    scheduler.enableSchedule(sched.getScheduleId());
    // add a schedule to be triggered every 10 minutes for SCHEDULED_PROG2_ID
    int sched2Mins = 10;
    sched = new ProgramSchedule("tsched2", "ten min schedule", SCHEDULED_PROG2_ID,
      Collections.EMPTY_MAP, new TimeTrigger(String.format("*/%d * * * *", sched2Mins)), Collections.emptyList());
    scheduler.addSchedule(sched);
    scheduler.enableSchedule(sched.getScheduleId());
    long currentTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    // get ops dashboard results between current time and current time + 3600 from TEST_NAMESPACE1 and TEST_NAMESPACE2
    int durationSecs = 3600;
    String opsDashboardQueryPath =
      String.format("%s/dashboard?start=%d&duration=%d&namespace=%s&namespace=%s",
        BASE_PATH, currentTime, durationSecs, TEST_NAMESPACE1, TEST_NAMESPACE2);
    List<DashboardProgramRunRecord> dashboardRecords = getDashboardRecords(opsDashboardQueryPath);
    // calculate the expected number of scheduled runs according to the query duration and the schedules
    long expectedScheduledProgram1 = durationSecs / TimeUnit.MINUTES.toSeconds(sched1Mins);
    long expectedScheduledProgram2 = durationSecs / TimeUnit.MINUTES.toSeconds(sched2Mins);
    // assert the number of scheduled runs are expected for both programs
    Assert.assertEquals(expectedScheduledProgram1,
      dashboardRecords.stream().filter(record -> SCHEDULED_PROG1_ID.getProgram().equals(record.getProgram())).count());
    Assert.assertEquals(expectedScheduledProgram2,
      dashboardRecords.stream().filter(record -> SCHEDULED_PROG2_ID.getProgram().equals(record.getProgram())).count());
  }

  @Test
  public void testDashboardDetail() throws Exception {
    // a path to get ops dashboard results between time 100 and 100 + 1440 = 1540 from namespaces
    // TEST_NAMESPACE1 and TEST_NAMESPACE2
    String opsDashboardQueryPath = String.format("%s/dashboard?start=100&duration=1440&namespace=%s&namespace=%s",
      BASE_PATH, TEST_NAMESPACE1, TEST_NAMESPACE2);
    // run1 will not be included in the query results since it stops before the query start time 100
    ProgramRunId run1 = PROG1_ID.run(RunIds.generate(TimeUnit.SECONDS.toMillis(10)));
    writeCompletedRunRecord(run1, ARTIFACT1_ID, 50);
    // run2 will be included in the query results since it starts before query end time
    // and stops after query start time
    ProgramRunId run2 = PROG1_ID.run(RunIds.generate(TimeUnit.SECONDS.toMillis(60)));
    writeCompletedRunRecord(run2, ARTIFACT1_ID, 110);
    // run3 will be included in the query results since it starts before query end time
    // and stops after query start time
    ProgramRunId run3 = PROG2_ID.run(RunIds.generate(TimeUnit.SECONDS.toMillis(120)));
    writeCompletedRunRecord(run3, ARTIFACT1_ID, 200);
    // run4 will be included in the query results since it starts before query end time
    // and stops after query start time
    ProgramRunId run4 = PROG2_ID.run(RunIds.generate(TimeUnit.SECONDS.toMillis(60)));
    writeCompletedRunRecord(run4, ARTIFACT1_ID, 2200);
    // run5 will be included in the query results since it starts before query end time
    // and stops after query start time
    ProgramRunId run5 = PROG2_ID.run(RunIds.generate(TimeUnit.SECONDS.toMillis(60)));
    store.setProvisioning(run5, Collections.emptyMap(), Collections.emptyMap(),
                          Bytes.toBytes(++sourceId), ARTIFACT1_ID);
    store.setStart(run5, "twillId", Collections.emptyMap(), Bytes.toBytes(++sourceId));
    // run6 will not be included in the query results since it starts after query end time
    ProgramRunId run6 = PROG2_ID.run(RunIds.generate(TimeUnit.SECONDS.toMillis(2000)));
    writeCompletedRunRecord(run6, ARTIFACT1_ID, 2200);
    // run7 will not be included in the query results since it does not belong the namespaces in the query
    ProgramRunId run7 = PROG3_ID.run(RunIds.generate(TimeUnit.SECONDS.toMillis(120)));
    writeCompletedRunRecord(run7, ARTIFACT1_ID, 200);
    // get ops dashboard query results
    List<DashboardProgramRunRecord> dashboardRecords = getDashboardRecords(opsDashboardQueryPath);
    // assert the result contains 4 entries and run2, run3, run4, and run5 are there
    Assert.assertEquals(4, dashboardRecords.size());
    Set<String> runs = dashboardRecords.stream().map(DashboardProgramRunRecord::getRun).collect(Collectors.toSet());
    Assert.assertEquals(ImmutableSet.of(run2.getRun(), run3.getRun(), run4.getRun(), run5.getRun()), runs);
    // set run5 to COMPLETED so that the namespace can be cleaned up
    store.setStop(run5, 200, ProgramRunStatus.COMPLETED, Bytes.toBytes(++sourceId));
  }

  private static List<DashboardProgramRunRecord> getDashboardRecords(String path) throws Exception {
    HttpResponse response = doGet(path);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String content = new String(ByteStreams.toByteArray(response.getEntity().getContent()), Charsets.UTF_8);
    return GSON.fromJson(content, DASHBOARD_DETAIL_TYPE);
  }

  private void writeCompletedRunRecord(ProgramRunId runId, ArtifactId artifactId, long endTime) {
    store.setProvisioning(runId, Collections.emptyMap(), Collections.emptyMap(),
                          Bytes.toBytes(++sourceId), artifactId);
    store.setProvisioned(runId, 0, Bytes.toBytes(++sourceId));
    store.setStart(runId, null, Collections.emptyMap(), Bytes.toBytes(++sourceId));
    store.setRunning(runId, endTime - 1, null, Bytes.toBytes(++sourceId));
    store.setStop(runId, endTime, ProgramRunStatus.COMPLETED, Bytes.toBytes(++sourceId));
  }
}
