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

package io.cdap.cdap.graphql.store.metadata;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import io.cdap.cdap.AppWithServices;
import io.cdap.cdap.graphql.CDAPGraphQLTest;
import io.cdap.cdap.graphql.cdap.schema.GraphQLFields;
import io.cdap.cdap.graphql.store.application.schema.ApplicationFields;
import io.cdap.cdap.graphql.store.metadata.schema.MetadataFields;
import io.cdap.cdap.proto.id.NamespaceId;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class MetadataDataFetcherTest extends CDAPGraphQLTest {

  @Test
  public void testGetMetadataRecord() throws Exception {
    deploy(AppWithServices.class, 200, null, NamespaceId.DEFAULT.getNamespace());

    String query = "{ "
      + "  application(name: \"" + AppWithServices.NAME + "\") {"
      + "    metadata {"
      + "      tags {"
      + "        name"
      + "        scope"
      + "      }"
      + "    }"
      + "  }"
      + "}";

    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(query).build();
    ExecutionResult executionResult = graphQL.execute(executionInput);

    Assert.assertTrue(executionResult.getErrors().isEmpty());

    Map<String, Map> data = executionResult.getData();
    Map<String, List> application = data.get(ApplicationFields.APPLICATION);
    Map<String, List> metadata = (Map<String, List>) application.get(ApplicationFields.METADATA);

    Map<String, String> tag = (Map<String, String>) metadata.get(MetadataFields.TAGS).get(0);
    Assert.assertNotNull(tag.get(GraphQLFields.NAME));
    Assert.assertNotNull(tag.get(MetadataFields.SCOPE));

    deleteAppAndData(NamespaceId.DEFAULT.app(AppWithServices.NAME));
  }

}
