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
import io.cdap.cdap.graphql.cdap.schema.GraphQLFields;
import io.cdap.cdap.graphql.store.artifact.schema.ArtifactFields;
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
      + "    }"
      + "  }"
      + "}";

    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(query).build();
    ExecutionResult executionResult = graphQL.execute(executionInput);

    Assert.assertTrue(executionResult.getErrors().isEmpty());

    Map<String, List> data = (Map<String, List>) executionResult.toSpecification().get(GraphQLFields.DATA);
    Assert.assertEquals(1, data.size());

    Map<String, List> artifactQuery = (Map<String, List>) data.get(GraphQLFields.ARTIFACT);
    List<Map> artifacts = artifactQuery.get(ArtifactFields.ARTIFACTS);
    Assert.assertEquals(1, artifacts.size());

    Map<String, Object> artifact = artifacts.get(0);
    Assert.assertNotNull(artifact.get("name"));

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
      + "      namespace {"
      + "        name"
      + "      }"
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

    Map<String, List> data = (Map<String, List>) executionResult.toSpecification().get(GraphQLFields.DATA);
    Assert.assertEquals(1, data.size());

    Map<String, Map> artifactQuery = (Map<String, Map>) data.get(GraphQLFields.ARTIFACT);
    Map<String, Object> artifact = artifactQuery.get(GraphQLFields.ARTIFACT);
    Assert.assertNotNull(artifact.get(GraphQLFields.NAME));
    Assert.assertNotNull(artifact.get(ArtifactFields.VERSION));
    Assert.assertNotNull(artifact.get(ArtifactFields.SCOPE));
    Assert.assertNotNull(artifact.get(GraphQLFields.NAMESPACE));
    Assert.assertNotNull(artifact.get(ArtifactFields.LOCATION));
    Assert.assertNotNull(artifact.get(ArtifactFields.PLUGINS));
    Assert.assertNotNull(artifact.get(ArtifactFields.APPLICATIONS));

    Map<String, String> namespace = (Map<String, String>) artifact.get(GraphQLFields.NAMESPACE);
    Assert.assertNotNull(namespace.get(GraphQLFields.NAME));

    Map<String, String> location = (Map<String, String>) artifact.get(ArtifactFields.LOCATION);
    Assert.assertNotNull(location.get(GraphQLFields.NAME));

    List<Map> plugins = (List<Map>) artifact.get(ArtifactFields.PLUGINS);
    Assert.assertTrue(plugins.isEmpty());
    // Map<String, String> plugin = (Map<String, String>) plugins.get(0);
    // Assert.assertNotNull(plugin.get(ArtifactFields.TYPE));
    // Assert.assertNotNull(plugin.get(GraphQLFields.NAME));
    // Assert.assertNotNull(plugin.get(ArtifactFields.DESCRIPTION));
    // Assert.assertNotNull(plugin.get(ArtifactFields.CLASS_NAME));
    // Assert.assertNotNull(plugin.get(ArtifactFields.CONFIG_FIELD_NAME));

    List<Map> applications = (List<Map>) artifact.get(ArtifactFields.APPLICATIONS);
    Map<String, String> application = (Map<String, String>) applications.get(0);
    Assert.assertNotNull(application.get(ArtifactFields.CLASS_NAME));
    Assert.assertNotNull(application.get(GraphQLFields.DESCRIPTION));

    System.out.println(executionResult.getData().toString());
  }

}
