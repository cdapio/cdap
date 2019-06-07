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
import io.cdap.cdap.graphql.CDAPGraphQLTest;
import io.cdap.cdap.graphql.cdap.schema.GraphQLFields;
import io.cdap.cdap.graphql.store.application.schema.ApplicationFields;
import io.cdap.cdap.graphql.store.programrecord.schema.ProgramRecordFields;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class ProgramRecordDataFetcherTest extends CDAPGraphQLTest {

  @Test
  public void testGetProgramRecords() {
    String query = "{ "
      + "  application(name: \"JavascriptTransform\") {"
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
      + "          id"
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
  public void testGetRuns() {
    String query = "{ "
      + "  application(name: \"Test\") {"
      + "    programs(type: \"Workflow\") {"
      + "      ... on Workflow {"
      + "        runs {"
      + "          pid"
      + "          startTs"
      + "          runTs"
      + "          stopTs"
      + "          suspendTs"
      + "          resumeTs"
      + "          status"
      + "          profileId"
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

    Map<String, String> run = (Map<String, String>) programRecord.get(ProgramRecordFields.RUNS).get(0);
    Assert.assertNotNull(run.get(ProgramRecordFields.PID));
    Assert.assertNotNull(run.get(ProgramRecordFields.START_TS));
    Assert.assertNotNull(run.get(ProgramRecordFields.RUN_TS));
    Assert.assertNotNull(run.get(ProgramRecordFields.STOP_TS));
    Assert.assertNotNull(run.get(ProgramRecordFields.STATUS));
    Assert.assertNotNull(run.get(ProgramRecordFields.PROFILE_ID));

    Assert.assertTrue(run.containsKey(ProgramRecordFields.SUSPEND_TS));
    Assert.assertTrue(run.containsKey(ProgramRecordFields.RESUME_TS));
  }

  @Test
  public void testGetSchedules() {
    String query = "{ "
      + "  application(name: \"JavascriptTransform\") {"
      + "    programs(type: \"Workflow\") {"
      + "      ... on Workflow {"
      + "        schedules {"
      + "          id"
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

    Map<String, String> startTime = (Map<String, String>) programRecord.get(ProgramRecordFields.SCHEDULES).get(0);
    Assert.assertNotNull(startTime.get(ProgramRecordFields.ID));
    Assert.assertNotNull(startTime.get(ProgramRecordFields.TIME));
  }

  @Test
  public void testGeMapReduceWithType() {
    String query = "{ "
      + "  application(name: \"JavascriptTransform\") {"
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
  }

}
