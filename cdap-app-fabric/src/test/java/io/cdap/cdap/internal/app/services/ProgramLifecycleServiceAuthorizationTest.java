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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.inject.Injector;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.schedule.ProgramSchedule;
import io.cdap.cdap.internal.app.runtime.schedule.ScheduleTaskRunner;
import io.cdap.cdap.internal.app.runtime.schedule.queue.Job;
import io.cdap.cdap.internal.app.runtime.schedule.queue.SimpleJob;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.security.AccessPermission;
import io.cdap.cdap.proto.security.ApplicationPermission;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.scheduler.ProgramScheduleService;
import io.cdap.cdap.security.authorization.AuthorizationUtil;
import io.cdap.cdap.security.authorization.InMemoryAccessControllerV2;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.PermissionManager;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test authorization for ProgramLifeCycleService.
 */
public class ProgramLifecycleServiceAuthorizationTest {

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  private static final Principal ALICE = new Principal("alice", Principal.PrincipalType.USER);

  private static CConfiguration cConf;
  private static Injector injector;
  private static PermissionManager permissionManager;
  private static AppFabricServer appFabricServer;
  private static AppFabricProcessorService appFabricProcessor;
  private static ProgramLifecycleService programLifecycleService;
  private static PreferencesService preferencesService;
  private static ProgramScheduleService programScheduleService;

  @BeforeClass
  public static void setup() throws Exception {
    cConf = createCconf();
    injector = AppFabricTestHelper.getInjector(cConf);
    permissionManager = injector.getInstance(PermissionManager.class);
    appFabricServer = injector.getInstance(AppFabricServer.class);
    appFabricServer.startAndWait();
    appFabricProcessor = injector.getInstance(AppFabricProcessorService.class);
    appFabricProcessor.startAndWait();
    programLifecycleService = injector.getInstance(ProgramLifecycleService.class);
    preferencesService = injector.getInstance(PreferencesService.class);
    programScheduleService = injector.getInstance(ProgramScheduleService.class);

    // Wait for the default namespace creation
    String user = AuthorizationUtil.getEffectiveMasterUser(cConf);
    permissionManager.grant(Authorizable.fromEntityId(NamespaceId.DEFAULT),
                            new Principal(user, Principal.PrincipalType.USER),
                            EnumSet.allOf(StandardPermission.class));
    // Starting the Appfabric server will create the default namespace
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return injector.getInstance(NamespaceAdmin.class).exists(NamespaceId.DEFAULT);
      }
    }, 5, TimeUnit.SECONDS);
    permissionManager.revoke(Authorizable.fromEntityId(NamespaceId.DEFAULT),
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
    permissionManager.grant(Authorizable.fromEntityId(applicationId.program(ProgramType.SERVICE,
                                                                            AllProgramsApp.NoOpService.NAME)), ALICE,
                            Collections.singleton(ApplicationPermission.EXECUTE));
    permissionManager.grant(Authorizable.fromEntityId(applicationId.program(ProgramType.WORKER,
                                                                            AllProgramsApp.NoOpWorker.NAME)), ALICE,
                            Collections.singleton(ApplicationPermission.EXECUTE));
    permissionManager.grant(Authorizable.fromEntityId(applicationId.program(ProgramType.SPARK,
                                                                            AllProgramsApp.NoOpSpark.NAME)), ALICE,
                            Collections.singleton(ApplicationPermission.EXECUTE));
    permissionManager.grant(Authorizable.fromEntityId(applicationId.program(ProgramType.MAPREDUCE,
                                                                            AllProgramsApp.NoOpMR.NAME)), ALICE,
                            Collections.singleton(ApplicationPermission.EXECUTE));
    permissionManager.grant(Authorizable.fromEntityId(applicationId.program(ProgramType.MAPREDUCE,
                                                                            AllProgramsApp.NoOpMR2.NAME)), ALICE,
                            Collections.singleton(ApplicationPermission.EXECUTE));
    permissionManager.grant(Authorizable.fromEntityId(applicationId.program(ProgramType.WORKFLOW,
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

    verifyRuntimeImpersonationEnforcement(applicationId);
  }

  private void verifyRuntimeImpersonationEnforcement(ApplicationId applicationId) throws Exception {
    SecurityRequestContext.setUserId(ALICE.getName());
    String deployedVersion =
        injector.getInstance(Store.class).getLatest(applicationId.getAppReference()).getSpec().getAppVersion();
    ProgramId workflowId =
        applicationId.program(ProgramType.WORKFLOW, AllProgramsApp.NoOpWorkflow.NAME);
    ProgramId deployedWorkflowId = new ProgramId(
        applicationId.getNamespace(), applicationId.getApplication(), deployedVersion,
        ProgramType.WORKFLOW, AllProgramsApp.NoOpWorkflow.NAME);
    permissionManager.grant(Authorizable.fromEntityId(workflowId), ALICE,
        EnumSet.allOf(StandardPermission.class));
    permissionManager.grant(Authorizable.fromEntityId(applicationId), ALICE,
        EnumSet.of(ApplicationPermission.EXECUTE));

    String targetPrincipal = "targetUser/host@REALM";
    KerberosPrincipalId targetPrincipalId = new KerberosPrincipalId(targetPrincipal);

    Map<String, String> principalOnlyArgs = ImmutableMap.of(
        SystemArguments.RUNTIME_PRINCIPAL_NAME, targetPrincipal);
    Map<String, String> principalAndKeytabArgs = ImmutableMap.of(
        SystemArguments.RUNTIME_PRINCIPAL_NAME, targetPrincipal,
        SystemArguments.RUNTIME_KEYTAB_PATH, "/tmp/target.keytab");
    Map<String, String> scopedPrincipalArgs = ImmutableMap.of(
        "workflow." + AllProgramsApp.NoOpWorkflow.NAME + "." + SystemArguments.RUNTIME_PRINCIPAL_NAME,
        targetPrincipal);

    // 1. Direct run, prepareStart, and saveRuntimeArgs must reject unauthorized runtime principal
    //    both with and without keytab path, and when scoped
    assertUnauthorized(() -> programLifecycleService.run(deployedWorkflowId, principalOnlyArgs, false));
    assertUnauthorized(() -> programLifecycleService.run(deployedWorkflowId, principalAndKeytabArgs, false));
    assertUnauthorized(() -> programLifecycleService.run(deployedWorkflowId, scopedPrincipalArgs, false));
    assertUnauthorized(() -> programLifecycleService.prepareStart(deployedWorkflowId, principalOnlyArgs, false, false));
    assertUnauthorized(() -> programLifecycleService.saveRuntimeArgs(deployedWorkflowId, principalOnlyArgs));
    assertUnauthorized(() -> programLifecycleService.saveRuntimeArgs(deployedWorkflowId, scopedPrincipalArgs));

    // 2. PreferencesService mutations must reject unauthorized runtime principal across all scopes
    assertUnauthorized(() -> preferencesService.setProperties(principalOnlyArgs));
    assertUnauthorized(() -> preferencesService.addProperties(principalOnlyArgs));
    assertUnauthorized(() -> preferencesService.setProperties(NamespaceId.DEFAULT, principalOnlyArgs));
    assertUnauthorized(() -> preferencesService.setProperties(applicationId, principalOnlyArgs));
    assertUnauthorized(() -> preferencesService.setProperties(workflowId, principalOnlyArgs));
    assertUnauthorized(() -> preferencesService.setProperties(workflowId, scopedPrincipalArgs));

    // 3. ProgramScheduleService add/update/resume must reject unauthorized runtime principal
    ProgramSchedule unauthorizedSchedule = new ProgramSchedule(
        "impersonationSched", "desc", workflowId,
        principalAndKeytabArgs, new TimeTrigger("0 * * * *"), Collections.emptyList());
    assertUnauthorized(() -> programScheduleService.add(unauthorizedSchedule));

    ProgramSchedule cleanSchedule = new ProgramSchedule(
        "impersonationSched", "desc", workflowId,
        Collections.emptyMap(), new TimeTrigger("0 * * * *"), Collections.emptyList());
    programScheduleService.add(cleanSchedule);
    try {
      ScheduleDetail updateWithPrincipal = new ScheduleDetail(
          "impersonationSched", "desc", null, principalOnlyArgs,
          new TimeTrigger("0 * * * *"), Collections.emptyList(), null);
      assertUnauthorized(() -> programScheduleService.update(cleanSchedule.getScheduleId(), updateWithPrincipal));
    } finally {
      programScheduleService.delete(cleanSchedule.getScheduleId());
    }

    // 4. ScheduleTaskRunner.launch must reject unauthorized runtime principal injected via schedule
    //    or trigger notification USER_OVERRIDES
    ScheduleTaskRunner taskRunner = new ScheduleTaskRunner(
        injector.getInstance(Store.class),
        programLifecycleService,
        injector.getInstance(PropertiesResolver.class),
        injector.getInstance(NamespaceQueryAdmin.class),
        cConf);
    Job directScheduleJob = new SimpleJob(
        unauthorizedSchedule, 0, 0L, Collections.emptyList(), Job.State.PENDING_LAUNCH, 0L);
    assertUnauthorized(() -> taskRunner.launch(directScheduleJob));

    Notification overrideNotification = new Notification(
        Notification.Type.TIME,
        ImmutableMap.of(
            ProgramOptionConstants.SCHEDULE_NAME, cleanSchedule.getName(),
            ProgramOptionConstants.SYSTEM_OVERRIDES,
            new Gson().toJson(ImmutableMap.of(ProgramOptionConstants.CRON_EXPRESSION, "0 * * * *")),
            ProgramOptionConstants.USER_OVERRIDES, new Gson().toJson(principalAndKeytabArgs)));
    Job notificationOverrideJob = new SimpleJob(
        cleanSchedule, 0, 0L, ImmutableList.of(overrideNotification), Job.State.PENDING_LAUNCH, 0L);
    assertUnauthorized(() -> taskRunner.launch(notificationOverrideJob));

    // 5. Granting AccessPermission.IMPERSONATE on targetPrincipalId permits configuration and execution
    permissionManager.grant(Authorizable.fromEntityId(targetPrincipalId), ALICE,
        Collections.singleton(AccessPermission.IMPERSONATE));
    try {
      programLifecycleService.saveRuntimeArgs(deployedWorkflowId, principalAndKeytabArgs);
      preferencesService.deleteProperties(deployedWorkflowId);

      programScheduleService.add(unauthorizedSchedule);
      programScheduleService.resume(unauthorizedSchedule.getScheduleId());
      programScheduleService.delete(unauthorizedSchedule.getScheduleId());
    } finally {
      permissionManager.revoke(Authorizable.fromEntityId(targetPrincipalId), ALICE,
          Collections.singleton(AccessPermission.IMPERSONATE));
    }
  }

  private void assertUnauthorized( ThrowingRunnable runnable) throws Exception {
    try {
      runnable.run();
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException expected) {
      // expected
    }
  }

  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  @AfterClass
  public static void tearDown() {
    appFabricServer.stopAndWait();
    appFabricProcessor.stopAndWait();
    AppFabricTestHelper.shutdown();
  }

  private static CConfiguration createCconf() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
    // we only want to test authorization, but we don't specify principal/keytab, so disable kerberos
    cConf.setBoolean(Constants.Security.KERBEROS_ENABLED, false);
    cConf.setInt(Constants.Security.Authorization.CACHE_MAX_ENTRIES, 0);
    LocationFactory locationFactory = new LocalLocationFactory(new File(TEMPORARY_FOLDER.newFolder().toURI()));
    Location authorizerJar = AppJarHelper.createDeploymentJar(locationFactory, InMemoryAccessControllerV2.class);
    cConf.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, authorizerJar.toURI().getPath());
    return cConf;
  }

  private void setUpPrivilegesAndExpectFailedDeploy(Map<EntityId, Set<? extends Permission>> neededPrivileges)
    throws Exception {
    int count = 0;
    for (Map.Entry<EntityId, Set<? extends Permission>> privilege : neededPrivileges.entrySet()) {
      permissionManager.grant(Authorizable.fromEntityId(privilege.getKey()), ALICE, privilege.getValue());
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
