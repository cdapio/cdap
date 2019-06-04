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
import io.cdap.cdap.StandaloneTester;
import io.cdap.cdap.cli.util.InstanceURIParser;
import io.cdap.cdap.client.NamespaceClient;
import io.cdap.cdap.client.common.ClientTestBase;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.graphql.cdap.provider.CDAPGraphQLProvider;
import io.cdap.cdap.graphql.cdap.runtimewiring.CDAPQueryTypeRuntimeWiring;
import io.cdap.cdap.graphql.cdap.schema.GraphQLSchemaFiles;
import io.cdap.cdap.graphql.provider.GraphQLProvider;
import io.cdap.cdap.graphql.store.artifact.runtimewiring.ArtifactQueryTypeRuntimeWiring;
import io.cdap.cdap.graphql.store.artifact.runtimewiring.ArtifactTypeRuntimeWiring;
import io.cdap.cdap.graphql.store.artifact.schema.ArtifactSchemaFiles;
import io.cdap.cdap.graphql.store.namespace.runtimewiring.NamespaceQueryTypeRuntimeWiring;
import io.cdap.cdap.graphql.store.namespace.runtimewiring.NamespaceTypeRuntimeWiring;
import io.cdap.cdap.graphql.store.namespace.schema.NamespaceSchemaFiles;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.test.SingletonExternalResource;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.List;

public abstract class CDAPQueryTypeRuntimeWiringTest {

  static GraphQL graphQL;
  static NamespaceClient namespaceClient;
  static ClientConfig clientConfig;

  @ClassRule
  public static final SingletonExternalResource STANDALONE = new SingletonExternalResource(
    new StandaloneTester(Constants.Explore.EXPLORE_ENABLED, false));

  // @ClassRule
  // public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  //
  // private static final Id.Artifact APP_ARTIFACT_ID = Id.Artifact.from(Id.Namespace.DEFAULT, "PluginTest", "1.0.0");
  // private static CConfiguration cConf;
  // private static File tmpDir;
  // private static File systemArtifactsDir1;
  // private static File systemArtifactsDir2;
  // private static ArtifactRepository artifactRepository;
  // private static File appArtifactFile;

  private static StandaloneTester getStandaloneTester() {
    return STANDALONE.get();
  }

  @BeforeClass
  public static void setup() throws Exception {
    StandaloneTester standalone = getStandaloneTester();
    ConnectionConfig connectionConfig = InstanceURIParser.DEFAULT.parse(standalone.getBaseURI().toString());
    clientConfig = new ClientConfig.Builder()
      .setDefaultReadTimeout(60 * 1000)
      .setUploadReadTimeout(120 * 1000)
      .setConnectionConfig(connectionConfig).build();

    namespaceClient = new NamespaceClient(clientConfig);
    //   systemArtifactsDir1 = TMP_FOLDER.newFolder();
    //   systemArtifactsDir2 = TMP_FOLDER.newFolder();
    //   tmpDir = TMP_FOLDER.newFolder();
    //
      cConf = CConfiguration.create();
    //   cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    //   cConf.set(Constants.AppFabric.SYSTEM_ARTIFACTS_DIR,
    //             systemArtifactsDir1.getAbsolutePath() + ";" + systemArtifactsDir2.getAbsolutePath());
    Injector injector = AppFabricTestHelper.getInjector();
    //   artifactRepository = injector.getInstance(ArtifactRepository.class);
    //
    //   appArtifactFile = createAppJar(PluginTestApp.class, new File(tmpDir, "PluginTest-1.0.0.jar"),
    //                                  createManifest(ManifestFields.EXPORT_PACKAGE,
    //                                                 PluginTestRunnable.class.getPackage().getName()));
    //
    //   injector.getInstance(NamespaceAdmin.class).create(NamespaceMeta.DEFAULT);

    List<String> schemaDefinitionFiles = Arrays.asList(GraphQLSchemaFiles.ROOT_SCHEMA,
                                                       ArtifactSchemaFiles.ARTIFACT_SCHEMA,
                                                       NamespaceSchemaFiles.NAMESPACE_SCHEMA);
    ArtifactQueryTypeRuntimeWiring artifactQueryTypeRuntimeWiring = injector
      .getInstance(ArtifactQueryTypeRuntimeWiring.class);
    ArtifactTypeRuntimeWiring artifactTypeRuntimeWiring = injector.getInstance(ArtifactTypeRuntimeWiring.class);
    CDAPQueryTypeRuntimeWiring cdapQueryTypeRuntimeWiring = injector.getInstance(CDAPQueryTypeRuntimeWiring.class);
    NamespaceQueryTypeRuntimeWiring namespaceQueryTypeRuntimeWiring = injector
      .getInstance(NamespaceQueryTypeRuntimeWiring.class);
    NamespaceTypeRuntimeWiring namespaceTypeRuntimeWiring = injector
      .getInstance(NamespaceTypeRuntimeWiring.class);
    GraphQLProvider graphQLProvider = new CDAPGraphQLProvider(schemaDefinitionFiles,
                                                              cdapQueryTypeRuntimeWiring,
                                                              artifactQueryTypeRuntimeWiring,
                                                              artifactTypeRuntimeWiring,
                                                              namespaceQueryTypeRuntimeWiring,
                                                              namespaceTypeRuntimeWiring);
    graphQL = graphQLProvider.buildGraphQL();
  }
  //
  // @AfterClass
  // public static void tearDown() {
  //   AppFabricTestHelper.shutdown();
  // }
  //
  // private static File createAppJar(Class<?> cls, File destFile, Manifest manifest) throws IOException {
  //   Location deploymentJar = AppJarHelper.createDeploymentJar(new LocalLocationFactory(TMP_FOLDER.newFolder()),
  //                                                             cls, manifest);
  //   DirUtils.mkdirs(destFile.getParentFile());
  //   Files.copy(Locations.newInputSupplier(deploymentJar), destFile);
  //   return destFile;
  // }
  //
  // private static Manifest createManifest(Object... entries) {
  //   Preconditions.checkArgument(entries.length % 2 == 0);
  //   Attributes attributes = new Attributes();
  //   for (int i = 0; i < entries.length; i += 2) {
  //     attributes.put(entries[i], entries[i + 1]);
  //   }
  //   Manifest manifest = new Manifest();
  //   manifest.getMainAttributes().putAll(attributes);
  //   return manifest;
  // }
  //
  // @Before
  // public void setupData() throws Exception {
  //   artifactRepository.clear(NamespaceId.DEFAULT);
  //   artifactRepository.addArtifact(APP_ARTIFACT_ID, appArtifactFile, null, null);
  // }
}
