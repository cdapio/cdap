/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.AllProgramsApp;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.ObjectMappedTable;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.id.Id;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.services.AppFabricServer;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Authorizable;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.authorization.AuthorizationUtil;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.cdap.security.authorization.InMemoryAuthorizer;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.spi.metadata.SearchRequest;
import com.google.inject.Injector;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Test authorization for metadata
 */
public class MetadataAdminAuthorizationTest {

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  private static final Principal ALICE = new Principal("alice", Principal.PrincipalType.USER);

  private static CConfiguration cConf;
  private static MetadataAdmin metadataAdmin;
  private static Authorizer authorizer;
  private static AppFabricServer appFabricServer;

  @BeforeClass
  public static void setup() throws Exception {
    cConf = createCConf();
    final Injector injector = AppFabricTestHelper.getInjector(cConf);
    metadataAdmin = injector.getInstance(MetadataAdmin.class);
    authorizer = injector.getInstance(AuthorizerInstantiator.class).get();
    appFabricServer = injector.getInstance(AppFabricServer.class);
    appFabricServer.startAndWait();

    // Wait for the default namespace creation
    String user = AuthorizationUtil.getEffectiveMasterUser(cConf);
    authorizer.grant(Authorizable.fromEntityId(NamespaceId.DEFAULT), new Principal(user, Principal.PrincipalType.USER),
                     Collections.singleton(Action.ADMIN));
    // Starting the Appfabric server will create the default namespace
    Tasks.waitFor(true, () -> injector.getInstance(NamespaceAdmin.class).exists(NamespaceId.DEFAULT),
                  5, TimeUnit.SECONDS);
    authorizer.revoke(Authorizable.fromEntityId(NamespaceId.DEFAULT), new Principal(user, Principal.PrincipalType.USER),
                      Collections.singleton(Action.ADMIN));
  }

  @Test
  public void testSearch() throws Exception {
    SecurityRequestContext.setUserId(ALICE.getName());
    ApplicationId applicationId = NamespaceId.DEFAULT.app(AllProgramsApp.NAME);
    // grant all the privileges needed to deploy the app
    authorizer.grant(Authorizable.fromEntityId(applicationId), ALICE, Collections.singleton(Action.ADMIN));
    authorizer.grant(Authorizable.fromEntityId(NamespaceId.DEFAULT.artifact(AllProgramsApp.class.getSimpleName(),
                                                                            "1.0-SNAPSHOT")),
                     ALICE, Collections.singleton(Action.ADMIN));
    authorizer.grant(Authorizable.fromEntityId(NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME)), ALICE,
                     Collections.singleton(Action.ADMIN));
    authorizer.grant(Authorizable.fromEntityId(NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME2)), ALICE,
                     Collections.singleton(Action.ADMIN));
    authorizer.grant(Authorizable.fromEntityId(NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME3)), ALICE,
                     Collections.singleton(Action.ADMIN));
    authorizer.grant(Authorizable.fromEntityId(NamespaceId.DEFAULT.dataset(AllProgramsApp.DS_WITH_SCHEMA_NAME)), ALICE,
                     Collections.singleton(Action.ADMIN));
    authorizer.grant(Authorizable.fromEntityId(NamespaceId.DEFAULT.datasetType(KeyValueTable.class.getName())), ALICE,
                     Collections.singleton(Action.ADMIN));
    authorizer.grant(Authorizable.fromEntityId(NamespaceId.DEFAULT.datasetType(KeyValueTable.class.getName())), ALICE,
                     Collections.singleton(Action.ADMIN));
    authorizer.grant(Authorizable.fromEntityId(NamespaceId.DEFAULT.datasetType(ObjectMappedTable.class.getName())),
                     ALICE,
                     Collections.singleton(Action.ADMIN));
    // no auto grant now, need to have privileges on the program to be able to see the programs
    authorizer.grant(Authorizable.fromEntityId(applicationId.program(ProgramType.SERVICE,
                                                                     AllProgramsApp.NoOpService.NAME)), ALICE,
                     Collections.singleton(Action.EXECUTE));
    authorizer.grant(Authorizable.fromEntityId(applicationId.program(ProgramType.WORKER,
                                                                     AllProgramsApp.NoOpWorker.NAME)), ALICE,
                     Collections.singleton(Action.EXECUTE));
    authorizer.grant(Authorizable.fromEntityId(applicationId.program(ProgramType.SPARK,
                                                                     AllProgramsApp.NoOpSpark.NAME)), ALICE,
                     Collections.singleton(Action.EXECUTE));
    authorizer.grant(Authorizable.fromEntityId(applicationId.program(ProgramType.MAPREDUCE,
                                                                     AllProgramsApp.NoOpMR.NAME)), ALICE,
                     Collections.singleton(Action.EXECUTE));
    authorizer.grant(Authorizable.fromEntityId(applicationId.program(ProgramType.MAPREDUCE,
                                                                     AllProgramsApp.NoOpMR2.NAME)), ALICE,
                     Collections.singleton(Action.EXECUTE));
    authorizer.grant(Authorizable.fromEntityId(applicationId.program(ProgramType.WORKFLOW,
                                                                     AllProgramsApp.NoOpWorkflow.NAME)), ALICE,
                     Collections.singleton(Action.EXECUTE));

    AppFabricTestHelper.deployApplication(Id.Namespace.DEFAULT, AllProgramsApp.class, "{}", cConf);

    SearchRequest searchRequest = SearchRequest.of("*")
      .addSystemNamespace()
      .addNamespace(NamespaceId.DEFAULT.getNamespace())
      // query for all metadata entity type except schedule
      // TODO (CDAP-14705): add back schedule type when the JIRA is fixed.
      .addType(MetadataEntity.NAMESPACE)
      .addType(MetadataEntity.ARTIFACT)
      .addType(MetadataEntity.APPLICATION)
      .addType(MetadataEntity.PROGRAM)
      .addType(MetadataEntity.DATASET)
      .setOffset(0)
      .setLimit(Integer.MAX_VALUE)
      .build();
    Tasks.waitFor(false, () -> metadataAdmin.search(searchRequest).getResults().isEmpty(), 5, TimeUnit.SECONDS);
    SecurityRequestContext.setUserId("bob");
    Assert.assertTrue(metadataAdmin.search(searchRequest).getResults().isEmpty());
  }

  @AfterClass
  public static void tearDown() {
    appFabricServer.stopAndWait();
  }

  private static CConfiguration createCConf() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
    // we only want to test authorization, but we don't specify principal/keytab, so disable kerberos
    cConf.setBoolean(Constants.Security.KERBEROS_ENABLED, false);
    cConf.setInt(Constants.Security.Authorization.CACHE_MAX_ENTRIES, 0);
    LocationFactory locationFactory = new LocalLocationFactory(new File(TEMPORARY_FOLDER.newFolder().toURI()));
    Location authorizerJar = AppJarHelper.createDeploymentJar(locationFactory, InMemoryAuthorizer.class);
    cConf.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, authorizerJar.toURI().getPath());
    return cConf;
  }
}
