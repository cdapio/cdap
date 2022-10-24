/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services.http.handlers;

import com.google.gson.Gson;
import io.cdap.cdap.WorkflowApp;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.workflow.ScheduleProgramInfo;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.schedule.constraint.DelayConstraint;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.proto.ApplicationRecord;
import io.cdap.cdap.proto.BatchProgramSchedule;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.ScheduledRuntime;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.common.http.HttpResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for scheduled run time.
 */
public class ScheduledRunTimeTest extends AppFabricTestBase {

  private static final Gson GSON = new Gson();
  private static final Id.Artifact ARTIFACT_ID = new Id.Artifact(Id.Namespace.DEFAULT, "test",
                                                                 new ArtifactVersion("1.0-SNAPSHOT"));

  @Before
  public void before() throws Exception {
    Assert.assertEquals(200, addAppArtifact(ARTIFACT_ID, WorkflowApp.class).getResponseCode());
  }

  @After
  public void after() throws Exception {
    deleteArtifact(ARTIFACT_ID, 200);
    getAppList(NamespaceId.DEFAULT.getNamespace()).stream()
      .map(o -> GSON.fromJson(o, ApplicationRecord.class))
      .map(r -> NamespaceId.DEFAULT.app(r.getName(), r.getAppVersion()))
      .forEach(id -> {
        try {
          deleteApp(id, 200);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
  }

  @Test
  public void testGetNextRun() throws Exception {
    ApplicationId appId = NamespaceId.DEFAULT.app("test");
    deploy(appId, new AppRequest<>(new ArtifactSummary(ARTIFACT_ID.getName(), ARTIFACT_ID.getVersion().getVersion())));

    String scheduleName = "schedule1";

    // Add a schedule. Use a constraint to make it not going to be executed
    ProgramId programId = appId.workflow(WorkflowApp.FunWorkflow.NAME);
    Constraint constraint = new DelayConstraint(1, TimeUnit.HOURS);
    ScheduleProgramInfo scheduleProgramInfo = new ScheduleProgramInfo(programId.getType().getSchedulableType(),
                                                                      programId.getProgram());
    addSchedule(appId.getNamespace(), appId.getApplication(), appId.getVersion(), scheduleName,
                new ScheduleDetail(scheduleName, null, scheduleProgramInfo, null,
                                   new TimeTrigger("0 0 * * * "), Collections.singletonList(constraint), null));

    long now = System.currentTimeMillis();
    HttpResponse response = enableSchedule(programId.getNamespace(), programId.getApplication(),
                                           programId.getVersion(), scheduleName);
    Assert.assertEquals(200, response.getResponseCode());

    // Get the next run time
    List<ScheduledRuntime> scheduledRunTimes = getScheduledRunTimes(programId, true);
    Assert.assertEquals(1, scheduledRunTimes.size());

    long nextTime = scheduledRunTimes.get(0).getTime();
    Assert.assertTrue(nextTime >= now);
  }

  @Test
  public void testBatchGetNextRun() throws Exception {
    // deploys 5 apps and create schedules for each of them
    long now = System.currentTimeMillis();
    List<ProgramId> programIds = new ArrayList<>();

    // Use a constraint to make it not going to be executed
    Constraint constraint = new DelayConstraint(1, TimeUnit.HOURS);
    for (int i = 0; i < 5; i++) {
      ApplicationId appId = NamespaceId.DEFAULT.app("test" + i);
      deploy(appId, new AppRequest<>(new ArtifactSummary(ARTIFACT_ID.getName(),
                                                         ARTIFACT_ID.getVersion().getVersion())));

      String scheduleName = "schedule" + i;

      // Add a schedule
      ProgramId programId = appId.workflow(WorkflowApp.FunWorkflow.NAME);
      programIds.add(programId);

      ScheduleProgramInfo scheduleProgramInfo = new ScheduleProgramInfo(programId.getType().getSchedulableType(),
                                                                        programId.getProgram());
      addSchedule(appId.getNamespace(), appId.getApplication(), appId.getVersion(), scheduleName,
                  new ScheduleDetail(scheduleName, null, scheduleProgramInfo, null,
                                     new TimeTrigger("0 0 * * * "), Collections.singletonList(constraint), null));
      HttpResponse response = enableSchedule(programId.getNamespace(), programId.getApplication(),
                                             programId.getVersion(), scheduleName);
      Assert.assertEquals(200, response.getResponseCode());
    }

    // Add programs that the app or the program doesn't exist
    programIds.add(NamespaceId.DEFAULT.app("not-exist").workflow("not-exist"));
    programIds.add(NamespaceId.DEFAULT.app("test1").workflow("not-exist"));
    List<BatchProgramSchedule> schedules = getScheduledRunTimes(NamespaceId.DEFAULT.getNamespace(), programIds, true);

    Assert.assertEquals(programIds.size(), schedules.size());

    // For the first 5 programs, they should have a next run
    for (int i = 0; i < 5; i++) {
      BatchProgramSchedule schedule = schedules.get(i);
      Assert.assertEquals(200, schedule.getStatusCode());
      List<ScheduledRuntime> nextRuns = schedule.getSchedules();
      Assert.assertNotNull(nextRuns);
      Assert.assertEquals(1, nextRuns.size());
      long nextTime = nextRuns.get(0).getTime();
      Assert.assertTrue(nextTime >= now);
    }

    // The last two should be a not found
    Assert.assertEquals(404, schedules.get(5).getStatusCode());
    Assert.assertEquals(404, schedules.get(6).getStatusCode());
  }
}
