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


import co.cask.cdap.api.schedule.TriggerInfo;
import co.cask.cdap.api.schedule.TriggeringScheduleInfo;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.codec.ArgumentsCodec;
import co.cask.cdap.internal.app.runtime.codec.ProgramOptionsCodec;
import co.cask.cdap.internal.app.runtime.schedule.DefaultTriggeringScheduleInfo;
import co.cask.cdap.internal.app.runtime.schedule.TriggeringScheduleInfoAdapter;
import co.cask.cdap.internal.app.runtime.schedule.trigger.DefaultTimeTriggerInfo;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.ops.DashboardProgramRunRecord;
import co.cask.cdap.reporting.ProgramHeartbeatStore;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.tephra.TransactionFailureException;
import org.apache.twill.api.RunId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Tests for {@link OperationsDashboardHttpHandler}
 */
public class OperationsDashboardHttpHandlerTest extends AppFabricTestBase {
  private static final ArtifactId ARTIFACT_ID = NamespaceId.DEFAULT.artifact("testArtifact", "1.0");
  private static final Gson GSON =
    TriggeringScheduleInfoAdapter.addTypeAdapters(new GsonBuilder())
      .registerTypeAdapter(Arguments.class, new ArgumentsCodec())
      .registerTypeAdapter(ProgramOptions.class, new ProgramOptionsCodec()).create();
  private static final String BASE_PATH = Constants.Gateway.API_VERSION_3;
  private static final Type DASHBOARD_DETAIL_TYPE = new TypeToken<List<DashboardProgramRunRecord>>() { }.getType();
  private static ProgramHeartbeatStore programHeartbeatStore;

  @BeforeClass
  public static void setup() throws Exception {
    programHeartbeatStore = getInjector().getInstance(ProgramHeartbeatStore.class);
  }

  /**
   * writes heart beat messages starting from startTime + interval up to endTime, each heartbeat separated by interval
   */
  private void setUpProgramHeartBeats(Map<String, String> properties,
                                      long startTime, long endTime, long interval,
                                      ProgramHeartbeatStore programHeartbeatStore)
    throws InterruptedException, TransactionFailureException {
    for (long time = startTime + interval; time < endTime; time += interval) {
      properties.put(ProgramOptionConstants.HEART_BEAT_TIME, String.valueOf(time));
      Notification heartbeat = new Notification(Notification.Type.HEART_BEAT, properties);
      programHeartbeatStore.writeNotification(heartbeat);
    }
  }

  /**
   * setup and return mock program properties with status running, runId and time when it started running are
   * specified by parameters
   */
  private Map<String, String> getMockProgramProperties(NamespaceId namespaceId, RunId runId, StatusTime statusTime,
                                                       @Nullable TriggeringScheduleInfo triggeringScheduleInfo) {
    ProgramId programId = namespaceId.app("someapp").program(ProgramType.SERVICE, "s");
    Map<String, String> systemArguments = new HashMap<>();
    systemArguments.put(ProgramOptionConstants.SKIP_PROVISIONING, Boolean.TRUE.toString());
    systemArguments.putIfAbsent(ProgramOptionConstants.ARTIFACT_ID, Joiner.on(":").join(ARTIFACT_ID.toIdParts()));
    systemArguments.put(ProgramOptionConstants.PRINCIPAL, "userA");
    if (triggeringScheduleInfo != null) {
      systemArguments.put(ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO, GSON.toJson(triggeringScheduleInfo));
    }
    ProgramOptions programOptions = new SimpleProgramOptions(programId, new BasicArguments(systemArguments),
                                                             new BasicArguments());

    Map<String, String> properties = new HashMap<>();

    properties.put(ProgramOptionConstants.PROGRAM_RUN_ID, GSON.toJson(programId.run(runId)));
    if (statusTime.getProgramStatus().isEndState()) {
      properties.put(ProgramOptionConstants.END_TIME, String.valueOf(statusTime.getTime()));
    } else if (statusTime.getProgramStatus().equals(ProgramRunStatus.RUNNING)) {
      properties.put(ProgramOptionConstants.LOGICAL_START_TIME, String.valueOf(statusTime.getTime()));
    } else if (statusTime.getProgramStatus().equals(ProgramRunStatus.SUSPENDED)) {
      properties.put(ProgramOptionConstants.SUSPEND_TIME, String.valueOf(statusTime.getTime()));
    }

    properties.put(ProgramOptionConstants.PROGRAM_STATUS, statusTime.getProgramStatus().name());
    properties.put(ProgramOptionConstants.PROGRAM_OPTIONS, GSON.toJson(programOptions));
    return properties;
  }

  @Test
  public void testDashboardReadFromHeartbeatStore() throws Exception {
    long startTime1 = System.currentTimeMillis();
    RunId runId1 = RunIds.generate();
    Map<String, String> properties =
      getMockProgramProperties(NamespaceId.DEFAULT,
                               runId1, new StatusTime(ProgramRunStatus.RUNNING, startTime1), null);
    Notification run1Notification = new Notification(Notification.Type.PROGRAM_STATUS, properties);
    programHeartbeatStore.writeNotification(run1Notification);


    // write heart beat messages for 10 minutes (every minute) for this program run.
    long endTime = startTime1 + TimeUnit.MINUTES.toMillis(10);
    long interval = TimeUnit.MINUTES.toMillis(1);
    setUpProgramHeartBeats(properties, startTime1, endTime, interval, programHeartbeatStore);

    // write end notification message
    properties =
      getMockProgramProperties(NamespaceId.DEFAULT, runId1, new StatusTime(ProgramRunStatus.COMPLETED, endTime), null);
    Notification endNotification = new Notification(Notification.Type.PROGRAM_STATUS, properties);
    programHeartbeatStore.writeNotification(endNotification);


    long startTime2 = startTime1 + TimeUnit.MINUTES.toMillis(5);
    NamespaceId ns1 =  new NamespaceId("ns1");
    RunId runId2 = RunIds.generate();
    properties =
      getMockProgramProperties(ns1, runId2, new StatusTime(ProgramRunStatus.RUNNING, startTime2), null);
    Notification run2Notification = new Notification(Notification.Type.PROGRAM_STATUS, properties);
    programHeartbeatStore.writeNotification(run2Notification);

    // write heart beat messages for 5 minutes (every minute) for this program run.
    // this program doesnt have endTime for this testOp
    setUpProgramHeartBeats(properties, startTime2, endTime, interval, programHeartbeatStore);

    String opsDashboardQueryPath =
      String.format("%s/dashboard?start=%s&duration=%s&namespace=%s&namespace=%s", BASE_PATH,
                    String.valueOf(TimeUnit.MILLISECONDS.toSeconds(startTime1)),
                    String.valueOf(TimeUnit.MILLISECONDS.toSeconds(endTime)),
                    NamespaceId.DEFAULT.getNamespace(), ns1.getNamespace());
    // get ops dashboard query results
    HttpResponse response = doGet(opsDashboardQueryPath);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String content = new String(ByteStreams.toByteArray(response.getEntity().getContent()), Charsets.UTF_8);
    List<DashboardProgramRunRecord> dashboardDetail = GSON.fromJson(content, DASHBOARD_DETAIL_TYPE);
    // assert the result contains 2 entries
    Assert.assertEquals(2, dashboardDetail.size());
    Iterator<DashboardProgramRunRecord> dashboardProgramRunRecordIterator = dashboardDetail.iterator();

    while (dashboardProgramRunRecordIterator.hasNext()) {
      DashboardProgramRunRecord dashboardProgramRunRecord = dashboardProgramRunRecordIterator.next();
      if (ProgramRunStatus.RUNNING.equals(dashboardProgramRunRecord.getStatus())) {
        // test the runId is runId2
        Assert.assertEquals(runId2.getId(), dashboardProgramRunRecord.getRun());
      } else {
        Assert.assertEquals(ProgramRunStatus.COMPLETED, dashboardProgramRunRecord.getStatus());
        Assert.assertEquals(runId1.getId(), dashboardProgramRunRecord.getRun());
      }
    }

    // for the same time range query only in namespace ns1 to ensure filtering works fine
    opsDashboardQueryPath =
      String.format("%s/dashboard?start=%s&duration=%s&namespace=%s", BASE_PATH,
                    String.valueOf(TimeUnit.MILLISECONDS.toSeconds(startTime1)),
                    String.valueOf(TimeUnit.MILLISECONDS.toSeconds(endTime)), ns1.getNamespace());

    // get ops dashboard query results
    response = doGet(opsDashboardQueryPath);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    content = new String(ByteStreams.toByteArray(response.getEntity().getContent()), Charsets.UTF_8);
    dashboardDetail = GSON.fromJson(content, DASHBOARD_DETAIL_TYPE);
    // assert the result contains 2 entries
    Assert.assertEquals(1, dashboardDetail.size());
    DashboardProgramRunRecord record = dashboardDetail.iterator().next();
    Assert.assertEquals(runId2.getId(), record.getRun());
    Assert.assertEquals(ProgramRunStatus.RUNNING, record.getStatus());
  }

  @Test
  public void testDashboardReadWithScheduledRuns() throws Exception {
    long startTime1 = System.currentTimeMillis();
    RunId runId1 = RunIds.generate();
    NamespaceId ns2 =  new NamespaceId("ns2");
    List<TriggerInfo> triggerInfos = new ArrayList<>();
    triggerInfos.add(new DefaultTimeTriggerInfo("*/5 * * * *", startTime1));
    TriggeringScheduleInfo triggeringScheduleInfo =
      new DefaultTriggeringScheduleInfo("test", "test", triggerInfos, new HashMap<>());
    Map<String, String> properties =
      getMockProgramProperties(ns2, runId1, new StatusTime(ProgramRunStatus.RUNNING, startTime1),
                               triggeringScheduleInfo);
    Notification run1Notification = new Notification(Notification.Type.PROGRAM_STATUS, properties);
    programHeartbeatStore.writeNotification(run1Notification);


    // write heart beat messages for 10 minutes (every minute) for this program run.
    long endTime = startTime1 + TimeUnit.MINUTES.toMillis(5);
    long interval = TimeUnit.MINUTES.toMillis(1);
    setUpProgramHeartBeats(properties, startTime1, endTime, interval, programHeartbeatStore);

    String opsDashboardQueryPath =
      String.format("%s/dashboard?start=%s&duration=%s&namespace=%s", BASE_PATH,
                    String.valueOf(TimeUnit.MILLISECONDS.toSeconds(startTime1)),
                    String.valueOf(TimeUnit.MILLISECONDS.toSeconds(endTime)), ns2.getNamespace());

    // get ops dashboard query results
    HttpResponse response = doGet(opsDashboardQueryPath);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String content = new String(ByteStreams.toByteArray(response.getEntity().getContent()), Charsets.UTF_8);
    List<DashboardProgramRunRecord> dashboardDetail = GSON.fromJson(content, DASHBOARD_DETAIL_TYPE);
    // assert the result contains 2 entries
    Assert.assertEquals(1, dashboardDetail.size());
    DashboardProgramRunRecord resultRecord = dashboardDetail.iterator().next();
    Assert.assertEquals(runId1.getId(), resultRecord.getRun());
    Assert.assertEquals(ProgramRunStatus.RUNNING, resultRecord.getStatus());
    Assert.assertEquals(TriggerInfo.Type.TIME.name(), resultRecord.getStartMethod());
  }


  class StatusTime {
    ProgramRunStatus programStatus;
    long time;

    StatusTime(ProgramRunStatus programRunStatus, long time) {
      this.programStatus = programRunStatus;
      this.time = time;
    }
    public ProgramRunStatus getProgramStatus() {
      return programStatus;
    }

    public long getTime() {
      return time;
    }
  }
}
