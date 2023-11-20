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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.SleepingWorkflowApp;
import io.cdap.cdap.SleepingWorkflowApp.SleepWorkflow;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.internal.profile.ProfileService;
import io.cdap.cdap.internal.provision.MockProvisioner;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.proto.provisioner.ProvisionerInfo;
import io.netty.handler.codec.http.HttpResponseStatus;
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
  private static CConfiguration cConf;

  @BeforeClass
  public static void beforeClass() throws Throwable {
    cConf = createBasicCconf();
    initializeAndStartServices(cConf);
    Injector injector = getInjector();
    programLifecycleService = injector.getInstance(ProgramLifecycleService.class);
    profileService = injector.getInstance(ProfileService.class);
    provisioningService = injector.getInstance(ProvisioningService.class);
    provisioningService.startAndWait();
  }

  @AfterClass
  public static void shutdown() {
    provisioningService.stopAndWait();
  }

  @Test
  public void testEmptyRunsIsStopped() {
    Assert.assertEquals(ProgramStatus.STOPPED, ProgramLifecycleService.getProgramStatus(Collections.emptyList()));
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
    ProgramStatus status = ProgramLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.STARTING, status);

    record = RunRecordDetail.builder(record).setStatus(ProgramRunStatus.STARTING).build();
    status = ProgramLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.STARTING, status);

    // running, suspended, resuming -> running
    record = RunRecordDetail.builder(record).setStatus(ProgramRunStatus.RUNNING).build();
    status = ProgramLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.RUNNING, status);

    record = RunRecordDetail.builder(record).setStatus(ProgramRunStatus.SUSPENDED).build();
    status = ProgramLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.RUNNING, status);

    // failed, killed, completed -> stopped
    record = RunRecordDetail.builder(record).setStatus(ProgramRunStatus.FAILED).build();
    status = ProgramLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.STOPPED, status);

    record = RunRecordDetail.builder(record).setStatus(ProgramRunStatus.KILLED).build();
    status = ProgramLifecycleService.getProgramStatus(Collections.singleton(record));
    Assert.assertEquals(ProgramStatus.STOPPED, status);

    record = RunRecordDetail.builder(record).setStatus(ProgramRunStatus.COMPLETED).build();
    status = ProgramLifecycleService.getProgramStatus(Collections.singleton(record));
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
    ProgramStatus status = ProgramLifecycleService.getProgramStatus(
      Arrays.asList(pending, starting, running, killed, failed, completed));
    Assert.assertEquals(ProgramStatus.RUNNING, status);

    // starting takes precedence over stopped
    status = ProgramLifecycleService.getProgramStatus(Arrays.asList(pending, killed, failed, completed));
    Assert.assertEquals(ProgramStatus.STARTING, status);
    status = ProgramLifecycleService.getProgramStatus(Arrays.asList(starting, killed, failed, completed));
    Assert.assertEquals(ProgramStatus.STARTING, status);

    // end states are stopped
    status = ProgramLifecycleService.getProgramStatus(Arrays.asList(killed, failed, completed));
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

  private void setLCMFlag(boolean lcmFlag) {
    cConf.setBoolean(FEATURE_FLAG_PREFIX + Feature.LIFECYCLE_MANAGEMENT_EDIT.getFeatureFlagString(), lcmFlag);
  }
}
