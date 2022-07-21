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

package io.cdap.cdap.internal.app.services;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.ApplicationPermission;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.authorization.AccessControllerInstantiator;
import io.cdap.cdap.security.authorization.AuthorizationUtil;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.AccessController;
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
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Test authorization for ProgramLifeCycleService
 */
public class ProgramLifecycleServiceAuthorizationTest {

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  private static final Principal ALICE = new Principal("alice", Principal.PrincipalType.USER);

  private static CConfiguration cConf;
  private static AccessController accessController;
  private static AppFabricServer appFabricServer;
  private static ProgramLifecycleService programLifecycleService;

  @BeforeClass
  public static void setup() throws Exception {
    cConf = createCConf();
    final Injector injector = AppFabricTestHelper.getInjector(cConf);
    accessController = injector.getInstance(AccessControllerInstantiator.class).get();
    appFabricServer = injector.getInstance(AppFabricServer.class);
    appFabricServer.startAndWait();
    programLifecycleService = injector.getInstance(ProgramLifecycleService.class);

    // Wait for the default namespace creation
    String user = AuthorizationUtil.getEffectiveMasterUser(cConf);
    accessController.grant(Authorizable.fromEntityId(NamespaceId.DEFAULT),
                           new Principal(user, Principal.PrincipalType.USER),
                           EnumSet.allOf(StandardPermission.class));
    // Starting the Appfabric server will create the default namespace
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return injector.getInstance(NamespaceAdmin.class).exists(NamespaceId.DEFAULT);
      }
    }, 5, TimeUnit.SECONDS);
    accessController.revoke(Authorizable.fromEntityId(NamespaceId.DEFAULT),
                            new Principal(user, Principal.PrincipalType.USER),
                            Collections.singleton(StandardPermission.UPDATE));
  }

  @Test
  public void testProgramList() throws Exception {
    SecurityRequestContext.setUserId(ALICE.getName());
    ApplicationId applicationId = NamespaceId.DEFAULT.app(AllProgramsApp.NAME);
    Map<EntityId, Set<? extends Permission>> neededPrivileges =
      ImmutableMap.<EntityId, Set<? extends Permission>>builder()
      .put(applicationId, EnumSet.allOf(StandardPermission.class))
      .put(NamespaceId.DEFAULT, EnumSet.of(StandardPermission.GET))
      .put(NamespaceId.DEFAULT.artifact(AllProgramsApp.class.getSimpleName(), "1.0-SNAPSHOT"),
           EnumSet.allOf(StandardPermission.class))
      .put(NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME), EnumSet.allOf(StandardPermission.class))
      .put(NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME2), EnumSet.allOf(StandardPermission.class))
      .put(NamespaceId.DEFAULT.dataset(AllProgramsApp.DATASET_NAME3), EnumSet.allOf(StandardPermission.class))
      .put(NamespaceId.DEFAULT.dataset(AllProgramsApp.DS_WITH_SCHEMA_NAME), EnumSet.allOf(StandardPermission.class))
      .build();
    setUpPrivilegesAndExpectFailedDeploy(neededPrivileges);

    // now we should be able to deploy
    AppFabricTestHelper.deployApplication(Id.Namespace.DEFAULT, AllProgramsApp.class, null, cConf);

    // no auto grant now, the list will be empty for all program types
    for (ProgramType type : ProgramType.values()) {
      if (!ProgramType.CUSTOM_ACTION.equals(type)) {
        Assert.assertTrue(programLifecycleService.list(NamespaceId.DEFAULT, type).isEmpty());
      }
    }

    // no auto grant now, need to have privileges on the program to be able to see the programs
    accessController.grant(Authorizable.fromEntityId(applicationId.program(ProgramType.SERVICE,
                                                                           AllProgramsApp.NoOpService.NAME)), ALICE,
                           Collections.singleton(ApplicationPermission.EXECUTE));
    accessController.grant(Authorizable.fromEntityId(applicationId.program(ProgramType.WORKER,
                                                                           AllProgramsApp.NoOpWorker.NAME)), ALICE,
                           Collections.singleton(ApplicationPermission.EXECUTE));
    accessController.grant(Authorizable.fromEntityId(applicationId.program(ProgramType.SPARK,
                                                                           AllProgramsApp.NoOpSpark.NAME)), ALICE,
                           Collections.singleton(ApplicationPermission.EXECUTE));
    accessController.grant(Authorizable.fromEntityId(applicationId.program(ProgramType.MAPREDUCE,
                                                                           AllProgramsApp.NoOpMR.NAME)), ALICE,
                           Collections.singleton(ApplicationPermission.EXECUTE));
    accessController.grant(Authorizable.fromEntityId(applicationId.program(ProgramType.MAPREDUCE,
                                                                           AllProgramsApp.NoOpMR2.NAME)), ALICE,
                           Collections.singleton(ApplicationPermission.EXECUTE));
    accessController.grant(Authorizable.fromEntityId(applicationId.program(ProgramType.WORKFLOW,
                                                                           AllProgramsApp.NoOpWorkflow.NAME)), ALICE,
                           Collections.singleton(ApplicationPermission.EXECUTE));

    for (ProgramType type : ProgramType.values()) {
      // Skip custom permission.
      // Skip flow (until flow is completely removed from ProgramType)
      if (!ProgramType.CUSTOM_ACTION.equals(type)) {
        Assert.assertFalse(programLifecycleService.list(NamespaceId.DEFAULT, type).isEmpty());
        SecurityRequestContext.setUserId("bob");
        Assert.assertTrue(programLifecycleService.list(NamespaceId.DEFAULT, type).isEmpty());
        SecurityRequestContext.setUserId("alice");
      }
    }
  }

  @AfterClass
  public static void tearDown() {
    appFabricServer.stopAndWait();
    AppFabricTestHelper.shutdown();
  }

  private static CConfiguration createCConf() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
    // we only want to test authorization, but we don't specify principal/keytab, so disable kerberos
    cConf.setBoolean(Constants.Security.KERBEROS_ENABLED, false);
    cConf.setInt(Constants.Security.Authorization.CACHE_MAX_ENTRIES, 0);
    LocationFactory locationFactory = new LocalLocationFactory(new File(TEMPORARY_FOLDER.newFolder().toURI()));
    Location authorizerJar = AppJarHelper.createDeploymentJar(locationFactory, InMemoryAccessController.class);
    cConf.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, authorizerJar.toURI().getPath());
    return cConf;
  }

  private void setUpPrivilegesAndExpectFailedDeploy(Map<EntityId, Set<? extends Permission>> neededPrivileges)
    throws Exception {
    int count = 0;
    for (Map.Entry<EntityId, Set<? extends Permission>> privilege : neededPrivileges.entrySet()) {
      accessController.grant(Authorizable.fromEntityId(privilege.getKey()), ALICE, privilege.getValue());
      count++;
      if (count < neededPrivileges.size()) {
        try {
          AppFabricTestHelper.deployApplication(Id.Namespace.DEFAULT, AllProgramsApp.class, null, cConf);
          Assert.fail();
        } catch (Exception e) {
          // expected
        }
      }
    }
  }
}
