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

package io.cdap.cdap.graphql.store.artifact.runtimewiring;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import io.cdap.cdap.graphql.cdap.runtimewiring.CDAPQueryTypeRuntimeWiringTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class ArtifactQueryTypeRuntimeWiringTest extends CDAPQueryTypeRuntimeWiringTest {

  @Test
  public void testArtifacts() {
    String query = "{"
      + "  artifact {"
      + "    artifacts {"
      + "      name"
      + "      version"
      + "      scope"
      + "      namespace"
      + "      location {"
      + "        name"
      + "      }"
      + "      plugins {"
      + "        type"
      + "        name"
      + "        description"
      + "        className"
      + "        configFieldName"
      + "      }"
      + "      applications {"
      + "        className"
      + "        description"
      + "      }"
      + "    }"
      + "  }"
      + "}";

    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(query).build();
    ExecutionResult executionResult = graphQL.execute(executionInput);

    Assert.assertTrue(executionResult.getErrors().isEmpty());

    Map<String, List> data = (Map<String, List>) executionResult.toSpecification().get("data");
    Assert.assertEquals(1, data.size());

    Map<String, List> artifactQuery = (Map<String, List>) data.get("artifact");
    List<Map> artifacts = artifactQuery.get("artifacts");
    Assert.assertEquals(1, artifacts.size());

    Map<String, Object> artifact = artifacts.get(0);
    Assert.assertNotNull(artifact.get("name"));
    Assert.assertNotNull(artifact.get("version"));
    Assert.assertNotNull(artifact.get("scope"));
    Assert.assertNotNull(artifact.get("namespace"));
    Assert.assertNotNull(artifact.get("location"));

    Map<String, String> location = (Map<String, String>) artifact.get("location");
    Assert.assertNotNull(location.get("name"));

    System.out.println(executionResult.getData().toString());
  }

  @Test
  public void testArtifact() {
    String query = "{ "
      + "  artifact {"
      + "    artifact(name: \"PluginTest\", version: \"1.0.0\") {"
      + "      name"
      + "      version"
      + "      scope"
      + "      namespace"
      + "    }"
      + "  }"
      + "}";

    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(query).build();
    ExecutionResult executionResult = graphQL.execute(executionInput);

    Assert.assertTrue(executionResult.getErrors().isEmpty());

    System.out.println(executionResult.getData().toString());
  }

}
