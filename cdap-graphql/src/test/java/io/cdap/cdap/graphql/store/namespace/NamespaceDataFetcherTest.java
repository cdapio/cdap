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

package io.cdap.cdap.graphql.store.namespace;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import io.cdap.cdap.graphql.CDAPGraphQLTest;
import io.cdap.cdap.graphql.store.namespace.schema.NamespaceFields;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class NamespaceDataFetcherTest extends CDAPGraphQLTest {

  @Test
  public void testGetMetadataRecord() {

    String query = "{ "
      + "  namespaces {"
      + "    name"
      + "    description"
      + "    generation"
      + "  }"
      + "}";

    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(query).build();
    ExecutionResult executionResult = graphQL.execute(executionInput);

    Assert.assertTrue(executionResult.getErrors().isEmpty());

    Map<String, List> data = executionResult.getData();
    List<Map> namespaces = data.get(NamespaceFields.NAMESPACES);

    Map<String, String> namespace = namespaces.get(0);
    Assert.assertNotNull(namespace.get(NamespaceFields.NAME));
    Assert.assertNotNull(namespace.get(NamespaceFields.DESCRIPTION));
    Assert.assertNotNull(namespace.get(NamespaceFields.GENERATION));
  }

}
