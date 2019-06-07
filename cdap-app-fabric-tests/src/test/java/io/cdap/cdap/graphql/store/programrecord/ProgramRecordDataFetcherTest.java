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
  public void testGetWorkflow() {
    String query = "{ "
      + "  application(name: \"JavascriptTransform\") {"
      + "    programs(type: \"Workflow\") {"
      + "      ... on Workflow {"
      + "        runs {"
      + "          pid"
      + "        }"
      + "        startTimes {"
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
    Assert.assertNotNull(programRecord.get(ProgramRecordFields.START_TIMES));
  }

}
