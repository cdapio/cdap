/*
 *
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.graphql.store.programrecord;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import io.cdap.cdap.AppWithSchedule;
import io.cdap.cdap.AppWithServices;
import io.cdap.cdap.AppWithWorkflow;
import io.cdap.cdap.graphql.CDAPGraphQLTest;
import io.cdap.cdap.graphql.cdap.schema.GraphQLFields;
import io.cdap.cdap.graphql.store.application.schema.ApplicationFields;
import io.cdap.cdap.graphql.store.programrecord.schema.ProgramRecordFields;
import io.cdap.cdap.internal.app.runtime.batch.AppWithMapReduce;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class ProgramRecordDataFetcherTest extends CDAPGraphQLTest {

  @Test
  public void testGetProgramRecords() throws Exception {
    deploy(AppWithServices.class, 200, null, NamespaceId.DEFAULT.getNamespace());

    String query = "{ "
      + "  application(name: \"" + AppWithServices.NAME + "\") {"
      + "    programs {"
      + "      type"
      + "      app"
      + "      name"
      + "      description"
      + "    }"
      + "  }"
      + "}";

    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(query).build();
    ExecutionResult executionResult = graphQL.execute(executionInput);

    Assert.assertTrue(executionResult.getErrors().isEmpty());

    Map<String, Map> data = executionResult.getData();
    Map<String, List> application = data.get(ApplicationFields.APPLICATION);
    List programs = application.get(ApplicationFields.PROGRAMS);

    Map<String, String> programRecord = (Map<String, String>) programs.get(0);
    Assert.assertNotNull(programRecord.get(ProgramRecordFields.TYPE));
    Assert.assertNotNull(programRecord.get(ProgramRecordFields.APP));
    Assert.assertNotNull(programRecord.get(GraphQLFields.NAME));
    Assert.assertNotNull(programRecord.get(ProgramRecordFields.DESCRIPTION));

    deleteAppAndData(NamespaceId.DEFAULT.app(AppWithServices.NAME));
  }

  @Test
  public void testGetWorkflowWithType() {
    String query = "{ "
      + "  application(name: \"JavascriptTransform\") {"
      + "    programs(type: \"Workflow\") {"
      + "      ... on Workflow {"
      + "        runs {"
      + "          pid"
      + "        }"
      + "        schedules {"
      + "          name"
      + "        }"
      + "      }"
      + "    }"
      + "  }"
      + "}";

    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(query).build();
    ExecutionResult executionResult = graphQL.execute(executionInput);

    Assert.assertTrue(executionResult.getErrors().isEmpty());

    Map<String, Map> data = executionResult.getData();
    Map<String, List> application = data.get(ApplicationFields.APPLICATION);
    List programs = application.get(ApplicationFields.PROGRAMS);

    Map<String, String> programRecord = (Map<String, String>) programs.get(0);
    Assert.assertNotNull(programRecord.get(ProgramRecordFields.RUNS));
    Assert.assertNotNull(programRecord.get(ProgramRecordFields.SCHEDULES));
  }

  @Test
  public void testGetRuns() throws Exception {
    deploy(AppWithWorkflow.class, 200, null, NamespaceId.DEFAULT.getNamespace());

    ApplicationId appId = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), AppWithWorkflow.NAME);
    ProgramId sampleWorkflow = appId.service(AppWithWorkflow.SampleWorkflow.NAME);
    Assert.assertEquals("STOPPED", getProgramStatus(sampleWorkflow));

    // startProgram(sampleWorkflow);
    // waitState(sampleWorkflow, "RUNNING");
    //
    // stopProgram(sampleWorkflow);
    // waitState(sampleWorkflow, "STOPPED");


    deleteAppAndData(NamespaceId.DEFAULT.app(AppWithWorkflow.NAME));


    /*
    Sample code to run a workflow
     */
    // deploy(AppWithServicesAndWorker.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    //
    // ApplicationId appId = new ApplicationId(TEST_NAMESPACE1, AppWithServicesAndWorker.NAME);
    // ProgramId noOpService = appId.service(AppWithServicesAndWorker.NO_OP_SERVICE);
    // ProgramId pingService = appId.service(AppWithServicesAndWorker.PING_SERVICE);
    // ProgramId noOpWorker = appId.worker(AppWithServicesAndWorker.NO_OP_WORKER);
    //
    // // all programs are stopped initially
    // Assert.assertEquals(STOPPED, getProgramStatus(noOpService));
    // Assert.assertEquals(STOPPED, getProgramStatus(pingService));
    // Assert.assertEquals(STOPPED, getProgramStatus(noOpWorker));
    //
    // // start both services and check the status
    // startProgram(noOpService);
    // waitState(noOpService, RUNNING);
    //
    // startProgram(pingService);
    // waitState(pingService, RUNNING);
    //
    // // start the worker and check that it is rejected
    // startProgram(noOpWorker, 409);
    // Tasks.waitFor(1, () -> getProgramRuns(noOpWorker, ProgramRunStatus.ALL).size(), 10, TimeUnit.SECONDS);
    // Assert.assertEquals(ProgramRunStatus.REJECTED, getProgramRuns(noOpWorker, ProgramRunStatus.ALL).get(0).getStatus());
    //
    // // stop one service
    // stopProgram(noOpService);
    // waitState(noOpService, STOPPED);
    //
    // // starting the worker should now succeed
    // startProgram(noOpWorker, 200);
    // Tasks.waitFor(1, () -> getProgramRuns(noOpWorker, ProgramRunStatus.COMPLETED).size(), 10, TimeUnit.SECONDS);
    //
    // // stop other service
    // stopProgram(pingService);
    // waitState(pingService, STOPPED);




    // String query = "{ "
    //   + "  application(name: \"Test\") {"
    //   + "    programs(type: \"Workflow\") {"
    //   + "      ... on Workflow {"
    //   + "        runs {"
    //   + "          pid"
    //   + "          startTs"
    //   + "          runTs"
    //   + "          stopTs"
    //   + "          suspendTs"
    //   + "          resumeTs"
    //   + "          status"
    //   + "          profileId"
    //   + "        }"
    //   + "      }"
    //   + "    }"
    //   + "  }"
    //   + "}";
    //
    // ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(query).build();
    // ExecutionResult executionResult = graphQL.execute(executionInput);
    //
    // Assert.assertTrue(executionResult.getErrors().isEmpty());
    //
    // Map<String, Map> data = executionResult.getData();
    // Map<String, List> application = data.get(ApplicationFields.APPLICATION);
    // List programs = application.get(ApplicationFields.PROGRAMS);
    // Map<String, List> programRecord = (Map<String, List>) programs.get(0);
    //
    // Map<String, String> run = (Map<String, String>) programRecord.get(ProgramRecordFields.RUNS).get(0);
    // Assert.assertNotNull(run.get(ProgramRecordFields.PID));
    // Assert.assertNotNull(run.get(ProgramRecordFields.START_TS));
    // Assert.assertNotNull(run.get(ProgramRecordFields.RUN_TS));
    // Assert.assertNotNull(run.get(ProgramRecordFields.STOP_TS));
    // Assert.assertNotNull(run.get(ProgramRecordFields.STATUS));
    // Assert.assertNotNull(run.get(ProgramRecordFields.PROFILE_ID));
    //
    // Assert.assertTrue(run.containsKey(ProgramRecordFields.SUSPEND_TS));
    // Assert.assertTrue(run.containsKey(ProgramRecordFields.RESUME_TS));
  }

  @Test
  public void testGetSchedules() throws Exception {
    deploy(AppWithSchedule.class, 200, null, NamespaceId.DEFAULT.getNamespace());

    String query = "{ "
      + "  application(name: \"" + AppWithSchedule.NAME + "\") {"
      + "    programs(type: \"Workflow\") {"
      + "      ... on Workflow {"
      + "        schedules {"
      + "          namespace"
      + "          application"
      + "          applicationVersion"
      + "          name"
      + "          description"
      + "          status"
      + "          time"
      + "        }"
      + "      }"
      + "    }"
      + "  }"
      + "}";

    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(query).build();
    ExecutionResult executionResult = graphQL.execute(executionInput);

    Assert.assertTrue(executionResult.getErrors().isEmpty());

    Map<String, Map> data = executionResult.getData();
    Map<String, List> application = data.get(ApplicationFields.APPLICATION);
    List programs = application.get(ApplicationFields.PROGRAMS);
    Map<String, List> programRecord = (Map<String, List>) programs.get(0);

    Map<String, String> schedules = (Map<String, String>) programRecord.get(ProgramRecordFields.SCHEDULES).get(0);
    Assert.assertNotNull(schedules.get(ProgramRecordFields.NAMESPACE));
    Assert.assertNotNull(schedules.get(ProgramRecordFields.APPLICATION));
    Assert.assertNotNull(schedules.get(ProgramRecordFields.APPLICATION_VERSION));
    Assert.assertNotNull(schedules.get(ProgramRecordFields.NAME));
    Assert.assertNotNull(schedules.get(ProgramRecordFields.DESCRIPTION));
    Assert.assertNotNull(schedules.get(ProgramRecordFields.STATUS));
    Assert.assertNotNull(schedules.get(ProgramRecordFields.TIME));

    deleteAppAndData(NamespaceId.DEFAULT.app(AppWithSchedule.NAME));
  }

  @Test
  public void testGeMapReduceWithType() throws Exception {
    deploy(AppWithMapReduce.class, 200, null, NamespaceId.DEFAULT.getNamespace());

    String query = "{ "
      + "  application(name: \"" + AppWithMapReduce.NAME + "\") {"
      + "    programs(type: \"MapReduce\") {"
      + "      type"
      + "      app"
      + "      name"
      + "      description"
      + "    }"
      + "  }"
      + "}";

    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(query).build();
    ExecutionResult executionResult = graphQL.execute(executionInput);

    Assert.assertTrue(executionResult.getErrors().isEmpty());

    Map<String, Map> data = executionResult.getData();
    Map<String, List> application = data.get(ApplicationFields.APPLICATION);
    List programs = application.get(ApplicationFields.PROGRAMS);

    Map<String, String> programRecord = (Map<String, String>) programs.get(0);
    Assert.assertNotNull(programRecord.get(ProgramRecordFields.TYPE));
    Assert.assertNotNull(programRecord.get(ProgramRecordFields.APP));
    Assert.assertNotNull(programRecord.get(GraphQLFields.NAME));
    Assert.assertNotNull(programRecord.get(ProgramRecordFields.DESCRIPTION));

    deleteAppAndData(NamespaceId.DEFAULT.app(AppWithMapReduce.NAME));
  }

}
