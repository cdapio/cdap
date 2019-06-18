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

import com.google.inject.Injector;
import graphql.GraphQL;
import io.cdap.cdap.graphql.cdap.provider.CDAPGraphQLProvider;
import io.cdap.cdap.graphql.cdap.schema.GraphQLSchemaFiles;
import io.cdap.cdap.graphql.cdap.typeruntimewiring.CDAPQueryTypeRuntimeWiring;
import io.cdap.cdap.graphql.provider.GraphQLProvider;
import io.cdap.cdap.graphql.store.application.schema.ApplicationSchemaFiles;
import io.cdap.cdap.graphql.store.application.typeruntimewiring.ApplicationDetailTypeRuntimeWiring;
import io.cdap.cdap.graphql.store.application.typeruntimewiring.ApplicationRecordTypeRuntimeWiring;
import io.cdap.cdap.graphql.store.artifact.schema.ArtifactSchemaFiles;
import io.cdap.cdap.graphql.store.metadata.schema.MetadataSchemaFiles;
import io.cdap.cdap.graphql.store.namespace.schema.NamespaceSchemaFiles;
import io.cdap.cdap.graphql.store.programrecord.schema.ProgramRecordSchemaFiles;
import io.cdap.cdap.graphql.store.programrecord.typeruntimewiring.ScheduleDetailTypeRuntimeWiring;
import io.cdap.cdap.graphql.store.programrecord.typeruntimewiring.WorkflowTypeRuntimeWiring;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.id.ApplicationId;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.List;

/**
 * Test class for GraphQL queries. The tests were executed with the CDAP sandbox running locally
 */
public class CDAPGraphQLTest extends AppFabricTestBase {

  protected static GraphQL graphQL;
  private static Injector injector;

  @BeforeClass
  public static void setup() throws Exception {
    injector = AppFabricTestHelper.getInjector();

    List<String> schemaDefinitionFiles = Arrays.asList(GraphQLSchemaFiles.ROOT_SCHEMA,
                                                       ApplicationSchemaFiles.APPLICATION_SCHEMA,
                                                       ProgramRecordSchemaFiles.PROGRAM_RECORD_SCHEMA,
                                                       ArtifactSchemaFiles.ARTIFACT_SCHEMA,
                                                       MetadataSchemaFiles.METADATA_SCHEMA,
                                                       NamespaceSchemaFiles.NAMESPACE_SCHEMA);

    CDAPQueryTypeRuntimeWiring cdapQueryTypeRuntimeWiring = injector.getInstance(CDAPQueryTypeRuntimeWiring.class);
    ApplicationRecordTypeRuntimeWiring applicationRecordTypeRuntimeWiring = injector
      .getInstance(ApplicationRecordTypeRuntimeWiring.class);
    ApplicationDetailTypeRuntimeWiring applicationDetailTypeRuntimeWiring = injector
      .getInstance(ApplicationDetailTypeRuntimeWiring.class);
    WorkflowTypeRuntimeWiring workflowTypeRuntimeWiring = injector.getInstance(WorkflowTypeRuntimeWiring.class);
    ScheduleDetailTypeRuntimeWiring scheduleDetailTypeRuntimeWiring = injector
      .getInstance(ScheduleDetailTypeRuntimeWiring.class);

    GraphQLProvider graphQLProvider = new CDAPGraphQLProvider(schemaDefinitionFiles,
                                                              cdapQueryTypeRuntimeWiring,
                                                              applicationRecordTypeRuntimeWiring,
                                                              applicationDetailTypeRuntimeWiring,
                                                              workflowTypeRuntimeWiring,
                                                              scheduleDetailTypeRuntimeWiring);
    graphQL = graphQLProvider.buildGraphQL();
  }

  @AfterClass
  public static void tearDown() {
    AppFabricTestHelper.shutdown();
  }

  protected void deleteAppAndData(ApplicationId applicationId) throws Exception {
    deleteApp(applicationId, 200);
    deleteNamespaceData(applicationId.getNamespace());
  }
}
