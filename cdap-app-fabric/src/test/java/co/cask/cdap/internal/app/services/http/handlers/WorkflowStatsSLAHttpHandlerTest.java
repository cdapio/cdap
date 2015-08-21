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

package co.cask.cdap.internal.app.services.http.handlers;

import co.cask.cdap.WorkflowApp;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.PercentileInformation;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.WorkflowStatistics;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.twill.api.RunId;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link co.cask.cdap.gateway.handlers.WorkflowStatsSLAHttpHandler}
 */
public class WorkflowStatsSLAHttpHandlerTest extends AppFabricTestBase {

  @Test
  public void testStatistics() throws Exception {

    Store store = getInjector().getInstance(DefaultStore.class);

    deploy(WorkflowApp.class);
    String workflowName = "FunWorkflow";
    String mapreduceName = "ClassicWordCount";
    String sparkName = "SparkWorkflowTest";

    Id.Program workflowProgram =
      Id.Workflow.from(Id.Namespace.DEFAULT, "WorkflowApp", ProgramType.WORKFLOW, workflowName);
    Id.Program mapreduceProgram =
      Id.Program.from(Id.Namespace.DEFAULT, "WorkflowApp", ProgramType.MAPREDUCE, mapreduceName);
    Id.Program sparkProgram =
      Id.Program.from(Id.Namespace.DEFAULT, "WorkflowApp", ProgramType.SPARK, sparkName);

    long startTime = System.currentTimeMillis();
    long currentTimeMillis = startTime;
    String outlierRunId = null;
    for (int i = 0; i < 10; i++) {
      // work-flow runs every 5 minutes
      currentTimeMillis = startTime + (i * TimeUnit.MINUTES.toMillis(5));
      RunId workflowRunId = RunIds.generate(currentTimeMillis);
      store.setStart(workflowProgram, workflowRunId.getId(), RunIds.getTime(workflowRunId, TimeUnit.SECONDS));

      // MR job starts 2 seconds after workflow started
      RunId mapreduceRunid = RunIds.generate(currentTimeMillis + TimeUnit.SECONDS.toMillis(2));
      store.setWorkflowProgramStart(mapreduceProgram, mapreduceRunid.getId(), workflowProgram.getId(),
                                    workflowRunId.getId(), mapreduceProgram.getId(),
                                    RunIds.getTime(mapreduceRunid, TimeUnit.SECONDS), null, null);
      store.setStop(mapreduceProgram, mapreduceRunid.getId(),
                    // map-reduce job ran for 17 seconds
                    TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis) + 19,
                    ProgramRunStatus.COMPLETED);

      // This makes sure that not all runs have Spark programs in them
      if (i < 5) {
        // spark starts 20 seconds after workflow starts
        RunId sparkRunid = RunIds.generate(currentTimeMillis + TimeUnit.SECONDS.toMillis(20));
        store.setWorkflowProgramStart(sparkProgram, sparkRunid.getId(), workflowProgram.getId(),
                                      workflowRunId.getId(), sparkProgram.getId(),
                                      RunIds.getTime(sparkRunid, TimeUnit.SECONDS), null, null);
        // spark job runs for 38 seconds
        long stopTime = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis) + 58;
        if (i == 4) {
          // spark job ran for 100 seconds. 62 seconds greater than avg.
          stopTime = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis) + 120;
        }
        store.setStop(sparkProgram, sparkRunid.getId(), stopTime, ProgramRunStatus.COMPLETED);
      }

      // workflow ran for 1 minute
      long workflowStopTime = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis) + 60;
      if (i == 4) {
        // spark job ran longer for this run
        workflowStopTime = TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis) + 122;
        outlierRunId = workflowRunId.getId();
      }

      store.setStop(workflowProgram, workflowRunId.getId(), workflowStopTime, ProgramRunStatus.COMPLETED);
    }

    String request = String.format("%s/namespaces/%s/apps/%s/workflows/%s/statistics?start=%s&end=%s" +
                                     "&percentile=%s",
                                   Constants.Gateway.API_VERSION_3, Id.Namespace.DEFAULT,
                                   WorkflowApp.class.getSimpleName(), workflowProgram.getId(),
                                   TimeUnit.MILLISECONDS.toSeconds(startTime),
                                   TimeUnit.MILLISECONDS.toSeconds(currentTimeMillis) + TimeUnit.MINUTES.toSeconds(2),
                                   "99");

    HttpResponse response = doGet(request);
    WorkflowStatistics workflowStatistics =
      readResponse(response, new TypeToken<WorkflowStatistics>() { }.getType());
    PercentileInformation percentileInformation = workflowStatistics.getPercentileInformationList().get(0);
    Assert.assertEquals(1, percentileInformation.getRunIdsOverPercentile().size());
    Assert.assertEquals(outlierRunId, percentileInformation.getRunIdsOverPercentile().get(0));
    Assert.assertEquals("5", workflowStatistics.getNodes().get(sparkName).get("runs"));

    request = String.format("%s/namespaces/%s/apps/%s/workflows/%s/statistics?start=%s&end=%s" +
                              "&percentile=%s&percentile=%s",
                            Constants.Gateway.API_VERSION_3, Id.Namespace.DEFAULT,
                            WorkflowApp.class.getSimpleName(), workflowProgram.getId(), "now", "0", "90", "95");

    response = doGet(request);
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.getCode(),
                        response.getStatusLine().getStatusCode());

    request = String.format("%s/namespaces/%s/apps/%s/workflows/%s/statistics?start=%s&end=%s" +
                              "&percentile=%s&percentile=%s",
                            Constants.Gateway.API_VERSION_3, Id.Namespace.DEFAULT,
                            WorkflowApp.class.getSimpleName(), workflowProgram.getId(), "now", "0", "90.0", "950");

    response = doGet(request);
    Assert.assertEquals(HttpResponseStatus.BAD_REQUEST.getCode(),
                        response.getStatusLine().getStatusCode());
  }
}
