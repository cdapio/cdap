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

package io.cdap.cdap.graphql.store.application;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import io.cdap.cdap.AppWithMultipleSchedules;
import io.cdap.cdap.AppWithServices;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.graphql.CDAPGraphQLTest;
import io.cdap.cdap.graphql.cdap.schema.GraphQLFields;
import io.cdap.cdap.graphql.store.application.schema.ApplicationFields;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class ApplicationDataFetcherTest extends CDAPGraphQLTest {

  @Before
  public void setupTest() throws Exception {
    deploy(AppWithServices.class, 200, null, NamespaceId.DEFAULT.getNamespace());
  }

  @After
  public void tearDownTest() throws Exception {
    deleteAppAndData(NamespaceId.DEFAULT.app(AppWithServices.NAME));
  }

  @Test
  public void testGetApplicationDetail() {
    String query = "{ "
      + "  application(name: \"" + AppWithServices.NAME + "\") {"
      + "    name"
      + "    appVersion"
      + "    description"
      + "    configuration"
      + "    ownerPrincipal"
      + "  }"
      + "}";

    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(query).build();
    ExecutionResult executionResult = graphQL.execute(executionInput);

    Assert.assertTrue(executionResult.getErrors().isEmpty());

    Map<String, Map> data = executionResult.getData();
    Map<String, String> application = data.get(ApplicationFields.APPLICATION);
    Assert.assertNotNull(application.get(GraphQLFields.NAME));
    Assert.assertNotNull(application.get(ApplicationFields.APP_VERSION));
    Assert.assertNotNull(application.get(ApplicationFields.DESCRIPTION));
    Assert.assertNotNull(application.get(ApplicationFields.CONFIGURATION));

    Assert.assertTrue(application.containsKey(ApplicationFields.OWNER_PRINCIPAL));
  }

  @Test
  public void testGetApplicationRecord() {
    String query = "{ "
      + "  applications {"
      + "    type"
      + "    name"
      + "    version"
      + "    description"
      + "    ownerPrincipal"
      + "  }"
      + "}";

    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(query).build();
    ExecutionResult executionResult = graphQL.execute(executionInput);

    Assert.assertTrue(executionResult.getErrors().isEmpty());

    Map<String, List> data = executionResult.getData();
    Map<String, String> application = (Map<String, String>) data.get(ApplicationFields.APPLICATIONS).get(0);
    Assert.assertNotNull(application.get(ApplicationFields.TYPE));
    Assert.assertNotNull(application.get(GraphQLFields.NAME));
    Assert.assertNotNull(application.get(ApplicationFields.VERSION));
    Assert.assertNotNull(application.get(ApplicationFields.DESCRIPTION));

    Assert.assertTrue(application.containsKey(ApplicationFields.OWNER_PRINCIPAL));
  }

  @Test
  public void testGetApplications() {
    String query = "{ "
      + "  applications {"
      + "    type"
      + "    name"
      + "    version"
      + "    description"
      + "    ownerPrincipal"
      + "    artifact {"
      + "      name"
      + "    }"
      + "    applicationDetail {"
      + "      programs(type: \"Workflow\") {"
      + "        name"
      + "        ... on Workflow {"
      + "          runs {"
      + "            status"
      + "            startTs"
      + "          }"
      + "          schedules {"
      + "            time"
      + "          }"
      + "        }"
      + "      }"
      + "      metadata {"
      + "        tags {"
      + "          name"
      + "          scope"
      + "        }"
      + "      }"
      + "    }"
      + "  }"
      + "}";

    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(query).build();
    ExecutionResult executionResult = graphQL.execute(executionInput);
    System.out.println(executionResult.getData().toString());

    Assert.assertTrue(executionResult.getErrors().isEmpty());
  }

  @Test
  public void testGetApplication() {
    String query = "{ "
      + "  application(name: \"" + AppWithServices.NAME + "\") {"
      + "    name"
      + "    appVersion"
      + "    description"
      // + "    configuration"
      + "    ownerPrincipal"
      + "    programs(type: \"Workflow\") {"
      + "      name"
      + "      type"
      + "      app"
      + "      description"
      + "      ... on Workflow {"
      + "        runs {"
      + "          status"
      + "          startTs"
      + "        }"
      + "        schedules {"
      + "          time"
      + "        }"
      + "      }"
      + "    }"
      + "    metadata {"
      + "      tags {"
      + "          name"
      + "          scope"
      + "      }"
      + "    }"
      + "  }"
      + "}";

    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(query).build();
    ExecutionResult executionResult = graphQL.execute(executionInput);
    System.out.println(executionResult.getData().toString());

    Assert.assertTrue(executionResult.getErrors().isEmpty());
  }

  @Test
  public void testPipelineUI() throws Exception {
    deploy(AppWithMultipleSchedules.class, 200, Constants.Gateway.API_VERSION_3_TOKEN,
           NamespaceId.DEFAULT.getNamespace());

    ApplicationId appId = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), AppWithMultipleSchedules.NAME);
    ProgramId sampleWorkflow = appId.workflow(AppWithMultipleSchedules.SOME_WORKFLOW);
    startProgram(sampleWorkflow);
    waitState(sampleWorkflow, "STOPPED");

    resumeSchedule(NamespaceId.DEFAULT.getNamespace(), AppWithMultipleSchedules.NAME, "AnotherSchedule1");

    String query = "{ "
      + "  applications {"
      + "    name"
      + "    artifact {"
      + "      name"
      + "    }"
      + "    applicationDetail {"
      + "      programs(type: \"Workflow\") {"
      + "        name"
      + "        ... on Workflow {"
      + "          runs {"
      + "            status"
      + "            startTs"
      + "          }"
      + "          schedules {"
      + "            name"
      + "            time"
      + "            status"
      + "          }"
      + "        }"
      + "      }"
      + "      metadata {"
      + "        tags {"
      + "          name"
      + "          scope"
      + "        }"
      + "      }"
      + "    }"
      + "  }"
      + "}";

    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(query).build();
    ExecutionResult executionResult = graphQL.execute(executionInput);
    System.out.println(executionResult.getData().toString());

    Assert.assertTrue(executionResult.getErrors().isEmpty());

    stopProgram(sampleWorkflow);
    waitState(sampleWorkflow, "STOPPED");
    Thread.sleep(3000);

    deleteAppAndData(NamespaceId.DEFAULT.app(AppWithMultipleSchedules.NAME));
  }

}
