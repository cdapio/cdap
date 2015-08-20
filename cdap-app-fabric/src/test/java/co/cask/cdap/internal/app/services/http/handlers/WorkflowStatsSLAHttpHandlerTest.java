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

    for (int i = 0; i < 10; i++) {
      RunId workflowRunId = RunIds.generate();
      store.setStart(workflowProgram, workflowRunId.getId(), RunIds.getTime(workflowRunId, TimeUnit.SECONDS));

      RunId mapreduceRunid = RunIds.generate();
      store.setWorkflowProgramStart(mapreduceProgram, mapreduceRunid.getId(), workflowProgram.getId(),
                                    workflowRunId.getId(), mapreduceProgram.getId(),
                                    RunIds.getTime(mapreduceRunid, TimeUnit.SECONDS), null, null);
      store.setStop(mapreduceProgram, mapreduceRunid.getId(),
                    TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()), ProgramRunStatus.COMPLETED);

      // This makes sure that not all runs have Spark programs in them
      if (i < 5) {
        RunId sparkRunid = RunIds.generate();
        store.setWorkflowProgramStart(sparkProgram, sparkRunid.getId(), workflowProgram.getId(),
                                      workflowRunId.getId(), sparkProgram.getId(),
                                      RunIds.getTime(sparkRunid, TimeUnit.SECONDS), null, null);
        store.setStop(sparkProgram, sparkRunid.getId(),
                      TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()), ProgramRunStatus.COMPLETED);
      }
      store.setStop(workflowProgram, workflowRunId.getId(),
                    TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()), ProgramRunStatus.COMPLETED);

      // This sleep is required so that the store is not overridden as the workflows can then
      // potentially start at the same time causing conflicts
      TimeUnit.SECONDS.sleep(1);
    }

    String request = String.format("%s/namespaces/%s/apps/%s/workflows/%s/statistics?start=%s&end=%s" +
                                     "&percentile=%s&percentile=%s",
                                   Constants.Gateway.API_VERSION_3, Id.Namespace.DEFAULT,
                                   WorkflowApp.class.getSimpleName(), workflowProgram.getId(), "0", "now", "90", "95");

    HttpResponse response = doGet(request);
    WorkflowStatistics workflowStatistics =
      readResponse(response, new TypeToken<WorkflowStatistics>() { }.getType());
    Assert.assertEquals(1, workflowStatistics.getPercentileInformationList().get(0).getRunIdsOverPercentile().size());
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
