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

package io.cdap.cdap.graphql.store.namespace.runtimewiring;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import io.cdap.cdap.graphql.cdap.runtimewiring.CDAPQueryTypeRuntimeWiringTest;
import io.cdap.cdap.graphql.cdap.schema.GraphQLFields;
import io.cdap.cdap.graphql.store.artifact.schema.ArtifactFields;
import io.cdap.cdap.graphql.store.namespace.schema.NamespaceFields;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class NamespaceQueryTypeRuntimeWiringTest extends CDAPQueryTypeRuntimeWiringTest {

  @Test
  public void testNamespaces() {
    String query = "{ "
      + "  namespace {"
      + "    namespaces {"
      + "      name"
      + "      description"
      + "      generation"
      + "      artifacts {"
      + "        name"
      + "      }"
      + "    }"
      + "  }"
      + "}";

    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(query).build();
    ExecutionResult executionResult = graphQL.execute(executionInput);

    Assert.assertTrue(executionResult.getErrors().isEmpty());

    Map<String, List> data = (Map<String, List>) executionResult.toSpecification().get(GraphQLFields.DATA);
    Assert.assertEquals(1, data.size());

    Map<String, List> namespaceQuery = (Map<String, List>) data.get(GraphQLFields.NAMESPACE);
    List<Map> namespaces = namespaceQuery.get(NamespaceFields.NAMESPACES);
    Map<String, Object> namespace = (Map<String, Object>) namespaces.get(0);
    Assert.assertNotNull(namespace.get(GraphQLFields.NAME));
    Assert.assertNotNull(namespace.get(GraphQLFields.DESCRIPTION));
    Assert.assertNotNull(namespace.get(NamespaceFields.GENERATION));
    Assert.assertNotNull(namespace.get(ArtifactFields.ARTIFACTS));

    List<Map> artifacts = (List<Map>) namespace.get(ArtifactFields.ARTIFACTS);
    Map artifact = artifacts.get(0);
    Assert.assertNotNull(artifact.get(GraphQLFields.NAME));

    System.out.println(executionResult.getData().toString());
  }
}
