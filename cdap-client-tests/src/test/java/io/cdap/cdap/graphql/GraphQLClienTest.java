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

package io.cdap.cdap.graphql;

import graphql.Assert;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import io.cdap.cdap.proto.NamespaceMeta;
import org.junit.Test;

import java.util.List;

public class GraphQLClienTest extends CDAPQueryTypeRuntimeWiringTest {

  @Test
  public void testNamespaces() throws Exception {
    List<NamespaceMeta> namespaces = namespaceClient.list();
    Assert.assertTrue(!namespaces.isEmpty());
  }

  @Test
  public void testGetNamespaces() {
    String query = "{ "
      + "  namespace {"
      + "    namespaces {"
      + "      name"
      + "    }"
      + "  }"
      + "}";

    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(query).build();
    ExecutionResult executionResult = graphQL.execute(executionInput);

    org.junit.Assert.assertTrue(executionResult.getErrors().isEmpty());

    // Map<String, List> data = (Map<String, List>) executionResult.toSpecification().get(GraphQLFields.DATA);
    // Assert.assertEquals(1, data.size());
    //
    // Map<String, List> namespaceQuery = (Map<String, List>) data.get(GraphQLFields.NAMESPACE);
    // List<Map> namespaces = namespaceQuery.get(NamespaceFields.NAMESPACES);
    // Map<String, Object> namespace = (Map<String, Object>) namespaces.get(0);
    // Assert.assertNotNull(namespace.get(GraphQLFields.NAME));

    System.out.println(executionResult.getData().toString());
  }

}
