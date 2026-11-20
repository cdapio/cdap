/*
 * Copyright © 2018-2019 Cask Data, Inc.
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
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.SleepingWorkflowApp;
import io.cdap.cdap.SleepingWorkflowApp.SleepWorkflow;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.credential.CredentialIdentityManager;
import io.cdap.cdap.internal.credential.CredentialProfileStore;
import io.cdap.cdap.internal.namespace.credential.GcpWorkloadIdentityUtil;
import io.cdap.cdap.internal.profile.ProfileService;
import io.cdap.cdap.internal.provision.MockProvisioner;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.EntityScope;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.credential.CredentialIdentity;
import io.cdap.cdap.proto.credential.CredentialProfile;
import io.cdap.cdap.proto.id.CredentialIdentityId;
import io.cdap.cdap.proto.id.CredentialProfileId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.proto.provisioner.ProvisionerInfo;
import io.cdap.cdap.proto.provisioner.ProvisionerPropertyValue;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * ProgramLifecycleService tests.
 */
public class ProgramLifecycleServiceTest extends AppFabricTestBase {
  private static final String FEATURE_FLAG_PREFIX = "feature.";

  private static ProgramLifecycleService programLifecycleService;
  private static ProfileService profileService;
  private static ProvisioningService provisioningService;
  private static CredentialProfileStore credentialProfileStore;
  private static CredentialIdentityManager credentialIdentityManager;
  private static TransactionRunner transactionRunner;
  private static CConfiguration cConf;

  @BeforeClass
  public static void beforeClass() throws Throwable {
    cConf = createBasicCconf();
    initializeAndStartServices(cConf);
    Injector injector = getInjector();
    programLifecycleService = injector.getInstance(ProgramLifecycleService.class);
    profileService = injector.getInstance(ProfileService.class);
    provisioningService = injector.getInstance(ProvisioningService.class);
    credentialProfileStore = injector.getInstance(CredentialProfileStore.class);
    credentialIdentityManager = injector.getInstance(CredentialIdentityManager.class);
    transactionRunner = injector.getInstance(TransactionRunner.class);
    provisioningService.startAndWait();
  }

  @AfterClass
  public static void shutdown() {
    provisioningService.stopAndWait();
  }

  @Test
  public void testEmptyRunsIsStopped() {
    Assert.assertEquals(ProgramStatus.STOPPED, programLifecycleService.getProgramStatus(Collections.emptyList()));
  }

  @Test
  public void testProgramStatusFromSingleRun() {
    RunRecordDetail record = RunRecordDetail.builder()
      .setProgramRunId(NamespaceId.DEFAULT.app("app").mr("mr").run(RunIds.generate()))
      .setStartTime(System.currentTimeMillis())
      .setArtifactId(new ArtifactId("r", new ArtifactVersion("1.0"), ArtifactScope.USER))
      .setStatus(ProgramRunStatus.PENDING)
      .setSourceId(new byte[] { 0 })
      .build();

    // pending or starting -> starting
    ProgramStatus status = programLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.STARTING, status);

    record = RunRecordDetail.builder(record).setStatus(ProgramRunStatus.STARTING).build();
    status = programLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.STARTING, status);

    // running, suspended, resuming -> running
    record = RunRecordDetail.builder(record).setStatus(ProgramRunStatus.RUNNING).build();
    status = programLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.RUNNING, status);

    record = RunRecordDetail.builder(record).setStatus(ProgramRunStatus.SUSPENDED).build();
    status = programLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.RUNNING, status);

    // failed, killed, completed -> stopped
    record = RunRecordDetail.builder(record).setStatus(ProgramRunStatus.FAILED).build();
    status = programLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.STOPPED, status);

    record = RunRecordDetail.builder(record).setStatus(ProgramRunStatus.KILLED).build();
    status = programLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.STOPPED, status);

    record = RunRecordDetail.builder(record).setStatus(ProgramRunStatus.COMPLETED).build();
    status = programLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.STOPPED, status);
  }

  @Test
  public void testProgramStatusFromMultipleRuns() {
    ProgramId programId = NamespaceId.DEFAULT.app("app").mr("mr");
    RunRecordDetail pending = RunRecordDetail.builder()
      .setProgramRunId(programId.run(RunIds.generate()))
      .setStartTime(System.currentTimeMillis())
      .setArtifactId(new ArtifactId("r", new ArtifactVersion("1.0"), ArtifactScope.USER))
      .setStatus(ProgramRunStatus.PENDING)
      .setSourceId(new byte[] { 0 })
      .build();
    RunRecordDetail starting = RunRecordDetail.builder(pending)
      .setProgramRunId(programId.run(RunIds.generate()))
      .setStatus(ProgramRunStatus.STARTING).build();
    RunRecordDetail running = RunRecordDetail.builder(pending)
      .setProgramRunId(programId.run(RunIds.generate()))
      .setStatus(ProgramRunStatus.RUNNING).build();
    RunRecordDetail killed = RunRecordDetail.builder(pending)
      .setProgramRunId(programId.run(RunIds.generate()))
      .setStatus(ProgramRunStatus.KILLED).build();
    RunRecordDetail failed = RunRecordDetail.builder(pending)
      .setProgramRunId(programId.run(RunIds.generate()))
      .setStatus(ProgramRunStatus.FAILED).build();
    RunRecordDetail completed = RunRecordDetail.builder(pending)
      .setProgramRunId(programId.run(RunIds.generate()))
      .setStatus(ProgramRunStatus.COMPLETED).build();

    // running takes precedence over others
    ProgramStatus status = programLifecycleService.getProgramStatus(
      Arrays.asList(pending, starting, running, killed, failed, completed));
    Assert.assertEquals(ProgramStatus.RUNNING, status);

    // starting takes precedence over stopped
    status = programLifecycleService.getProgramStatus(Arrays.asList(pending, killed, failed, completed));
    Assert.assertEquals(ProgramStatus.STARTING, status);
    status = programLifecycleService.getProgramStatus(Arrays.asList(starting, killed, failed, completed));
    Assert.assertEquals(ProgramStatus.STARTING, status);

    // end states are stopped
    status = programLifecycleService.getProgramStatus(Arrays.asList(killed, failed, completed));
    Assert.assertEquals(ProgramStatus.STOPPED, status);
  }

  @Test
  public void testCreateProgramOptions() throws Exception {
    deploy(AllProgramsApp.class, 200);
    ApplicationDetail applicationDetail = getAppDetails(NamespaceId.DEFAULT.getNamespace(), AllProgramsApp.NAME);
    ProgramId programId = NamespaceId.DEFAULT
      .app(AllProgramsApp.NAME, applicationDetail.getAppVersion())
      .program(ProgramType.SPARK, AllProgramsApp.NoOpSpark.NAME);
    ProgramOptions options = programLifecycleService.createProgramOptions(programId, Collections.emptyMap(),
                                                                          Collections.emptyMap(), false);
    Assert.assertEquals(ProjectInfo.getVersion().toString(),
                        options.getArguments().getOption(Constants.APP_CDAP_VERSION));
  }

  @Test
  public void testProfileProgramTypeRestrictions() throws Exception {
    deploy(AllProgramsApp.class, 200);
    ApplicationDetail applicationDetail = getAppDetails(NamespaceId.DEFAULT.getNamespace(), AllProgramsApp.NAME);
    ProfileId profileId = NamespaceId.DEFAULT.profile("profABC");
    ProvisionerInfo provisionerInfo = new ProvisionerInfo(MockProvisioner.NAME, Collections.emptyList());
    Profile profile = new Profile("profABC", "label", "desc", provisionerInfo);
    profileService.createIfNotExists(profileId, profile);

    try {
      Map<String, String> userArgs = new HashMap<>();
      userArgs.put(SystemArguments.PROFILE_NAME, profileId.getProfile());
      Map<String, String> systemArgs = new HashMap<>();

      Set<ProgramId> programIds = ImmutableSet.of(
        NamespaceId.DEFAULT.app(AllProgramsApp.NAME, applicationDetail.getAppVersion())
          .program(ProgramType.SPARK, AllProgramsApp.NoOpSpark.NAME),
        NamespaceId.DEFAULT.app(AllProgramsApp.NAME, applicationDetail.getAppVersion())
          .program(ProgramType.MAPREDUCE, AllProgramsApp.NoOpMR.NAME),
        NamespaceId.DEFAULT.app(AllProgramsApp.NAME, applicationDetail.getAppVersion())
          .program(ProgramType.SERVICE, AllProgramsApp.NoOpService.NAME),
        NamespaceId.DEFAULT.app(AllProgramsApp.NAME, applicationDetail.getAppVersion())
          .program(ProgramType.WORKER, AllProgramsApp.NoOpWorker.NAME)
      );

      Set<ProgramType> allowCustomProfiles = EnumSet.of(ProgramType.MAPREDUCE, ProgramType.SPARK,
                                                        ProgramType.WORKFLOW, ProgramType.WORKER);
      for (ProgramId programId : programIds) {
        ProgramOptions options = programLifecycleService.createProgramOptions(programId, userArgs,
                                                                              systemArgs, false);
        Optional<ProfileId> opt = SystemArguments.getProfileIdFromArgs(NamespaceId.DEFAULT,
                                                                       options.getArguments().asMap());
        Assert.assertTrue(opt.isPresent());
        if (allowCustomProfiles.contains(programId.getType())) {
          Assert.assertEquals(profileId, opt.get());
        } else {
          Assert.assertEquals(ProfileId.NATIVE, opt.get());
        }
      }
    } finally {
      profileService.disableProfile(profileId);
      profileService.deleteProfile(profileId);
    }
  }

  @Test
  public void testMultiVersionProgramActions() throws Exception {
    setLCMFlag(true);
    // deploy, check the status
    deploy(SleepingWorkflowApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN,
           TEST_NAMESPACE1);
    ApplicationDetail appDetail = getAppDetails(TEST_NAMESPACE1, SleepingWorkflowApp.NAME);
    ProgramId programIdV1 = new NamespaceId(TEST_NAMESPACE1).app(SleepingWorkflowApp.NAME, appDetail.getAppVersion())
      .workflow(SleepWorkflow.NAME);
    // starting program v1
    startProgram(programIdV1, HttpResponseStatus.OK.code());
    waitState(programIdV1, "STARTING");

    // deploy the app again for a new version
    deploy(SleepingWorkflowApp.class, HttpResponseStatus.OK.code(), Constants.Gateway.API_VERSION_3_TOKEN,
           TEST_NAMESPACE1);
    appDetail = getAppDetails(TEST_NAMESPACE1, SleepingWorkflowApp.NAME);
    ProgramId programIdV2 = new NamespaceId(TEST_NAMESPACE1).app(SleepingWorkflowApp.NAME, appDetail.getAppVersion())
      .workflow(SleepWorkflow.NAME);

    // stop old version program should be ok
    stopProgram(programIdV1);
    // starting old program version is not allowed
    startProgram(programIdV1, HttpResponseStatus.BAD_REQUEST.code());

    // start/stop the latest version program should be ok
    startProgram(programIdV2);
    waitState(programIdV2, "STARTING");
    stopProgram(programIdV2);
    waitState(programIdV2, "STOPPED");

    // check the total run count is 1
    Assert.assertEquals(2L, getProgramRunCount(programIdV2.getProgramReference()));
    setLCMFlag(false);
  }

  @Test
  public void testStrictNsaUnboundNamespaceFailsClosed() throws Exception {
    deploy(AllProgramsApp.class, 200);
    ApplicationDetail appDetail = getAppDetails(NamespaceId.DEFAULT.getNamespace(),
        AllProgramsApp.NAME);
    ProgramId programId = NamespaceId.DEFAULT
        .app(AllProgramsApp.NAME, appDetail.getAppVersion())
        .program(ProgramType.SPARK, AllProgramsApp.NoOpSpark.NAME);
    ProfileId dataprocProfileId = NamespaceId.SYSTEM.profile("dataproc");
    Profile dataprocProfile = new Profile("dataproc", "Dataproc", "desc", EntityScope.SYSTEM,
        new ProvisionerInfo(MockProvisioner.NAME, Collections.emptyList()));
    profileService.createIfNotExists(dataprocProfileId, dataprocProfile);

    setStrictNsaFlag(true);
    try {
      Map<String, String> userArgs = ImmutableMap.of(
          SystemArguments.PROFILE_NAME, dataprocProfileId.getScopedName());
      try {
        programLifecycleService.createProgramOptions(programId, userArgs, Collections.emptyMap(),
            false);
        Assert.fail("Expected UnauthorizedException for unbound namespace under strict NSA mode");
      } catch (UnauthorizedException e) {
        Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, e.getStatusCode());
        Assert.assertTrue(e.getMessage().contains(
            "No Namespace Service Account is configured for namespace 'default'"));
      }
    } finally {
      setStrictNsaFlag(false);
      profileService.disableProfile(dataprocProfileId);
      profileService.deleteProfile(dataprocProfileId);
    }
  }

  @Test
  public void testStrictNsaInjectsBoundNsaOnDataprocProfile() throws Exception {
    deploy(AllProgramsApp.class, 200);
    ApplicationDetail appDetail = getAppDetails(NamespaceId.DEFAULT.getNamespace(),
        AllProgramsApp.NAME);
    ProgramId programId = NamespaceId.DEFAULT
        .app(AllProgramsApp.NAME, appDetail.getAppVersion())
        .program(ProgramType.SPARK, AllProgramsApp.NoOpSpark.NAME);
    ProfileId dataprocProfileId = NamespaceId.SYSTEM.profile("dataproc");
    Profile dataprocProfile = new Profile("dataproc", "Dataproc", "desc", EntityScope.SYSTEM,
        new ProvisionerInfo(MockProvisioner.NAME, Collections.emptyList()));
    profileService.createIfNotExists(dataprocProfileId, dataprocProfile);

    String boundNsa = "nsa-default@my-proj.iam.gserviceaccount.com";
    CredentialIdentityId identityId = bindNamespaceServiceAccount(NamespaceId.DEFAULT, boundNsa);
    setStrictNsaFlag(true);
    SecurityRequestContext.setUserId("alice");
    SecurityRequestContext.setUserIp("10.0.0.5");
    try {
      Map<String, String> userArgs = ImmutableMap.of(
          SystemArguments.PROFILE_NAME, dataprocProfileId.getScopedName());
      ProgramOptions options = programLifecycleService.createProgramOptions(programId, userArgs,
          Collections.emptyMap(), false);
      Assert.assertEquals(boundNsa,
          options.getArguments().getOption(
              SystemArguments.PROFILE_PROPERTIES_PREFIX + "serviceAccount"));
      Assert.assertEquals("alice", SecurityRequestContext.getUserId());
      Assert.assertEquals("10.0.0.5", SecurityRequestContext.getUserIp());
    } finally {
      SecurityRequestContext.reset();
      setStrictNsaFlag(false);
      deleteCredentialIdentityQuietly(identityId);
      profileService.disableProfile(dataprocProfileId);
      profileService.deleteProfile(dataprocProfileId);
    }
  }

  @Test
  public void testStrictNsaRejectsMismatchedServiceAccountOverrides() throws Exception {
    deploy(AllProgramsApp.class, 200);
    ApplicationDetail appDetail = getAppDetails(NamespaceId.DEFAULT.getNamespace(),
        AllProgramsApp.NAME);
    ProgramId programId = NamespaceId.DEFAULT
        .app(AllProgramsApp.NAME, appDetail.getAppVersion())
        .program(ProgramType.SPARK, AllProgramsApp.NoOpSpark.NAME);
    ProfileId dataprocProfileId = NamespaceId.SYSTEM.profile("dataproc");
    Profile dataprocProfile = new Profile("dataproc", "Dataproc", "desc", EntityScope.SYSTEM,
        new ProvisionerInfo(MockProvisioner.NAME, Collections.emptyList()));
    profileService.createIfNotExists(dataprocProfileId, dataprocProfile);

    ProfileId customMismatchedProfileId = NamespaceId.DEFAULT.profile("profMismatchedSa");
    Profile customMismatchedProfile = new Profile("profMismatchedSa", "label", "desc",
        new ProvisionerInfo(MockProvisioner.NAME, Collections.singletonList(
            new ProvisionerPropertyValue("serviceAccount",
                "other-sa@my-proj.iam.gserviceaccount.com", false))));
    profileService.createIfNotExists(customMismatchedProfileId, customMismatchedProfile);

    String boundNsa = "nsa-default@my-proj.iam.gserviceaccount.com";
    String profileSaKey = SystemArguments.PROFILE_PROPERTIES_PREFIX + "serviceAccount";
    CredentialIdentityId identityId = bindNamespaceServiceAccount(NamespaceId.DEFAULT, boundNsa);
    setStrictNsaFlag(true);
    try {
      // 1. Mismatched serviceAccount override in userArgs must fail with 403.
      Map<String, String> mismatchedUserArgs = ImmutableMap.of(
          SystemArguments.PROFILE_NAME, dataprocProfileId.getScopedName(),
          profileSaKey, "p4sa@tenant-tp.iam.gserviceaccount.com");
      try {
        programLifecycleService.createProgramOptions(programId, mismatchedUserArgs,
            Collections.emptyMap(), false);
        Assert.fail("Expected UnauthorizedException for mismatched userArgs serviceAccount");
      } catch (UnauthorizedException e) {
        Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, e.getStatusCode());
        Assert.assertTrue(e.getMessage().contains(
            "Execution service account 'p4sa@tenant-tp.iam.gserviceaccount.com' must match the "
                + "Namespace Service Account '" + boundNsa + "'"));
      }

      // 2. Mismatched serviceAccount configured on custom profile must fail with 403.
      Map<String, String> mismatchedProfileArgs = ImmutableMap.of(
          SystemArguments.PROFILE_NAME, customMismatchedProfileId.getScopedName());
      try {
        programLifecycleService.createProgramOptions(programId, mismatchedProfileArgs,
            Collections.emptyMap(), false);
        Assert.fail("Expected UnauthorizedException for mismatched profile serviceAccount");
      } catch (UnauthorizedException e) {
        Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, e.getStatusCode());
        Assert.assertTrue(e.getMessage().contains(
            "Execution service account 'other-sa@my-proj.iam.gserviceaccount.com' must match the "
                + "Namespace Service Account '" + boundNsa + "'"));
      }

      // 3. Matching serviceAccount (case-insensitive) succeeds and normalizes BOTH systemArgs and
      // userArgs to exact boundNsa.
      Map<String, String> matchingUserArgs = ImmutableMap.of(
          SystemArguments.PROFILE_NAME, dataprocProfileId.getScopedName(),
          profileSaKey, "NSA-DEFAULT@my-proj.iam.gserviceaccount.com");
      ProgramOptions options = programLifecycleService.createProgramOptions(programId,
          matchingUserArgs, Collections.emptyMap(), false);
      Assert.assertEquals(boundNsa, options.getArguments().getOption(profileSaKey));
      Assert.assertEquals(boundNsa, options.getUserArguments().getOption(profileSaKey));

      // 4. Blank "" or whitespace-only userArgs override is overwritten to exact boundNsa so it can
      // never clear systemArgs at runtime.
      for (String blankVal : new String[] {"", "   "}) {
        Map<String, String> blankUserArgs = ImmutableMap.of(
            SystemArguments.PROFILE_NAME, dataprocProfileId.getScopedName(),
            profileSaKey, blankVal,
            "spark.*." + profileSaKey, blankVal);
        ProgramOptions blankOptions = programLifecycleService.createProgramOptions(programId,
            blankUserArgs, Collections.emptyMap(), false);
        Assert.assertEquals(boundNsa, blankOptions.getArguments().getOption(profileSaKey));
        Assert.assertEquals(boundNsa, blankOptions.getUserArguments().getOption(profileSaKey));
        Assert.assertEquals(
            boundNsa, blankOptions.getUserArguments().getOption("spark.*." + profileSaKey));
      }

      // 5. Scoped runtime argument mismatch (e.g. spark.system.profile.properties.serviceAccount or
      // spark.*.system.profile.properties.serviceAccount) must fail with 403 UnauthorizedException.
      for (String scopedKey : new String[] {"spark." + profileSaKey, "spark.*." + profileSaKey}) {
        Map<String, String> scopedMismatchArgs = ImmutableMap.of(
            SystemArguments.PROFILE_NAME, dataprocProfileId.getScopedName(),
            scopedKey, "rogue-sa@my-proj.iam.gserviceaccount.com");
        try {
          programLifecycleService.createProgramOptions(programId, scopedMismatchArgs,
              Collections.emptyMap(), false);
          Assert.fail("Expected UnauthorizedException for scoped key: " + scopedKey);
        } catch (UnauthorizedException e) {
          Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, e.getStatusCode());
        }
      }

      // 6. ProfileId.NATIVE is exempt from strict NSA requirement even when no NSA is bound.
      deleteCredentialIdentityQuietly(identityId);
      Map<String, String> nativeArgs = ImmutableMap.of(
          SystemArguments.PROFILE_NAME, ProfileId.NATIVE.getScopedName());
      ProgramOptions nativeOptions = programLifecycleService.createProgramOptions(programId,
          nativeArgs, Collections.emptyMap(), false);
      Assert.assertEquals(
          "ON_PREMISE",
          nativeOptions.getArguments().getOption(ProgramOptionConstants.CLUSTER_MODE));
    } finally {
      setStrictNsaFlag(false);
      deleteCredentialIdentityQuietly(identityId);
      profileService.disableProfile(customMismatchedProfileId);
      profileService.deleteProfile(customMismatchedProfileId);
      profileService.disableProfile(dataprocProfileId);
      profileService.deleteProfile(dataprocProfileId);
    }
  }

  private CredentialIdentityId bindNamespaceServiceAccount(NamespaceId namespaceId,
      String serviceAccount) throws Exception {
    CredentialProfileId credProfileId = new CredentialProfileId(
        NamespaceId.SYSTEM.getNamespace(), GcpWorkloadIdentityUtil.SYSTEM_PROFILE_NAME);
    TransactionRunners.run(transactionRunner, context -> {
      if (!credentialProfileStore.exists(context, credProfileId)) {
        credentialProfileStore.write(context, credProfileId, new CredentialProfile(
            "gcp-wi-credential-provider", "test profile", Collections.emptyMap()));
      }
    }, Exception.class);
    CredentialIdentityId identityId = new CredentialIdentityId(
        NamespaceId.SYSTEM.getNamespace(),
        GcpWorkloadIdentityUtil.getWorkloadIdentityName(namespaceId));
    CredentialIdentity identity = new CredentialIdentity(
        NamespaceId.SYSTEM.getNamespace(), GcpWorkloadIdentityUtil.SYSTEM_PROFILE_NAME,
        "ns-ksa", serviceAccount);
    try {
      credentialIdentityManager.create(identityId, identity);
    } catch (AlreadyExistsException e) {
      credentialIdentityManager.update(identityId, identity);
    }
    return identityId;
  }

  private void deleteCredentialIdentityQuietly(CredentialIdentityId identityId) {
    try {
      credentialIdentityManager.delete(identityId);
    } catch (NotFoundException | java.io.IOException ignored) {
      // Ignore cleanup errors.
    }
  }

  private void setStrictNsaFlag(boolean enabled) {
    cConf.setBoolean(
        FEATURE_FLAG_PREFIX + Feature.NAMESPACED_SERVICE_ACCOUNTS_STRICT.getFeatureFlagString(),
        enabled);
  }

  private void setLCMFlag(boolean lcmFlag) {
    cConf.setBoolean(FEATURE_FLAG_PREFIX + Feature.LIFECYCLE_MANAGEMENT_EDIT.getFeatureFlagString(), lcmFlag);
  }
}
