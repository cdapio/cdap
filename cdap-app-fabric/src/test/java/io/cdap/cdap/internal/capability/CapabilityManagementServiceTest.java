/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.capability;

import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.CapabilityAppWithWorkflow;
import io.cdap.cdap.CapabilitySleepingWorkflowApp;
import io.cdap.cdap.CapabilitySleepingWorkflowPluginApp;
import io.cdap.cdap.WorkflowAppWithFork;
import io.cdap.cdap.api.annotation.Requirements;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.app.program.ManifestFields;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.test.PluginJarHelper;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

/**
 * Test for CapabilityManagementService
 */
public class CapabilityManagementServiceTest extends AppFabricTestBase {

  private static ArtifactRepository artifactRepository;
  private static LocationFactory locationFactory;
  private static CConfiguration cConfiguration;
  private static CapabilityManagementService capabilityManagementService;
  private static ApplicationLifecycleService applicationLifecycleService;
  private static ProgramLifecycleService programLifecycleService;
  private static CapabilityStatusStore capabilityStatusStore;
  private static final Gson GSON = new Gson();

  @BeforeClass
  public static void setup() {
    locationFactory = getInjector().getInstance(LocationFactory.class);
    artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    cConfiguration = getInjector().getInstance(CConfiguration.class);
    capabilityManagementService = getInjector().getInstance(CapabilityManagementService.class);
    capabilityStatusStore = new CapabilityStatusStore(getInjector().getInstance(TransactionRunner.class));
    applicationLifecycleService = getInjector().getInstance(ApplicationLifecycleService.class);
    programLifecycleService = getInjector().getInstance(ProgramLifecycleService.class);
    capabilityManagementService.stopAndWait();
  }

  @AfterClass
  public static void stop() {
    AppFabricTestHelper.shutdown();
  }

  @After
  public void reset() throws Exception {
    // Reset all relevant stores.
    for (ApplicationDetail appDetail : applicationLifecycleService
      .getApps(NamespaceId.SYSTEM, applicationDetail -> true)) {
      programLifecycleService.stopAll(
        new ApplicationId(NamespaceId.SYSTEM.getNamespace(), appDetail.getName(), appDetail.getAppVersion()));
    }
    for (ApplicationDetail appDetail : applicationLifecycleService
      .getApps(NamespaceId.DEFAULT, applicationDetail -> true)) {
      programLifecycleService.stopAll(
        new ApplicationId(NamespaceId.DEFAULT.getNamespace(), appDetail.getName(), appDetail.getAppVersion()));
    }
    applicationLifecycleService.removeAll(NamespaceId.SYSTEM);
    applicationLifecycleService.removeAll(NamespaceId.DEFAULT);
    artifactRepository.clear(NamespaceId.SYSTEM);
    artifactRepository.clear(NamespaceId.DEFAULT);
  }

  @Test
  public void testCapabilityManagement() throws Exception {
    String appName = AllProgramsApp.NAME;
    String programName = AllProgramsApp.NoOpService.NAME;
    Class<AllProgramsApp> appClass = AllProgramsApp.class;
    String version = "1.0.0";
    String namespace = NamespaceId.SYSTEM.getNamespace();
    //deploy the artifact
    deployTestArtifact(namespace, appName, version, appClass);

    ApplicationId applicationId = new ApplicationId(namespace, appName, version);
    ProgramId programId = new ProgramId(applicationId, ProgramType.SERVICE, programName);
    String externalConfigPath = tmpFolder.newFolder("capability-config-test").getAbsolutePath();
    cConfiguration.set(Constants.Capability.CONFIG_DIR, externalConfigPath);
    String fileName = "cap1.json";
    File testJson = new File(
      CapabilityManagementServiceTest.class.getResource(String.format("/%s", fileName)).getPath());
    Files.copy(testJson, new File(externalConfigPath, fileName));
    capabilityManagementService.runTask();
    List<JsonObject> appList = getAppList(namespace);
    Assert.assertFalse(appList.isEmpty());
    // Capability management service might not yet have deployed application.
    // So wait till program exists and is in running state.
    waitState(programId, "RUNNING");
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 1);

    //remove the file and make sure it gets removed
    new File(externalConfigPath, fileName).delete();
    capabilityManagementService.runTask();
    Assert.assertTrue(getAppList(namespace).isEmpty());

    //insert a pending entry, this should get re-applied and enable the capability
    FileReader fileReader = new FileReader(testJson);
    CapabilityConfig capabilityConfig = GSON.fromJson(fileReader, CapabilityConfig.class);
    capabilityStatusStore.addOrUpdateCapabilityOperation("cap1", CapabilityAction.ENABLE, capabilityConfig);
    capabilityManagementService.runTask();
    capabilityStatusStore.checkAllEnabled(Collections.singleton("cap1"));
    Assert.assertEquals(0, capabilityStatusStore.getCapabilityRecords().values().stream()
      .filter(capabilityRecord -> capabilityRecord.getCapabilityOperationRecord() != null).count());

    //pending task out of the way , on next run should be deleted
    capabilityManagementService.runTask();
    try {
      capabilityStatusStore.checkAllEnabled(Collections.singleton("cap1"));
      Assert.fail("expecting exception");
    } catch (CapabilityNotAvailableException ex) {

    }

    //disable from delete and see if it is applied correctly
    CapabilityConfig disableConfig = new CapabilityConfig(capabilityConfig.getLabel(), CapabilityStatus.DISABLED,
                                                          capabilityConfig.getCapability(),
                                                          capabilityConfig.getApplications(),
                                                          capabilityConfig.getPrograms(),
                                                          capabilityConfig.getHubs());
    writeConfigAsFile(externalConfigPath, fileName, disableConfig);
    capabilityManagementService.runTask();
    try {
      capabilityStatusStore.checkAllEnabled(Collections.singleton("cap1"));
      Assert.fail("expecting exception");
    } catch (CapabilityNotAvailableException ex) {

    }
    Assert.assertEquals(disableConfig, capabilityStatusStore.getConfigs(Collections.singleton("cap1")).get("cap1"));

    //enable again
    writeConfigAsFile(externalConfigPath, fileName, capabilityConfig);
    capabilityManagementService.runTask();
    capabilityStatusStore.checkAllEnabled(Collections.singleton("cap1"));
    // Capability management service might not yet have deployed application.
    // So wait till program exists and is in running state.
    waitState(programId, "RUNNING");
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 1);

    //cleanup
    new File(externalConfigPath, fileName).delete();
    capabilityManagementService.runTask();
  }

  private void writeConfigAsFile(String externalConfigPath, String fileName,
                                 CapabilityConfig disableConfig) throws IOException {
    try (FileWriter writer = new FileWriter(new File(externalConfigPath, fileName))) {
      GSON.toJson(disableConfig, writer);
    }
  }

  @Test
  public void testCapabilityRefresh() throws Exception {
    String externalConfigPath = tmpFolder.newFolder("capability-config-refresh").getAbsolutePath();
    cConfiguration.set(Constants.Capability.CONFIG_DIR, externalConfigPath);

    String appName = AllProgramsApp.NAME;
    String programName = AllProgramsApp.NoOpService.NAME;
    Class<AllProgramsApp> appClass = AllProgramsApp.class;
    String version = "1.0.0";
    String namespace = NamespaceId.SYSTEM.getNamespace();
    //deploy the artifact
    deployTestArtifact(namespace, appName, version, appClass);

    ApplicationId applicationId = new ApplicationId(namespace, appName, version);
    ProgramId programId = new ProgramId(applicationId, ProgramType.SERVICE, programName);
    //check that app is not available
    List<JsonObject> appList = getAppList(namespace);
    Assert.assertTrue(appList.isEmpty());

    //enable the capability
    CapabilityConfig config = getTestConfig("1.0.0");
    writeConfigAsFile(externalConfigPath, config.getCapability(), config);
    capabilityManagementService.runTask();
    //app should show up and program should have run
    appList = getAppList(namespace);
    Assert.assertFalse(appList.isEmpty());
    // Capability management service might not yet have deployed application.
    // So wait till program exists and is in running state.
    waitState(programId, "RUNNING");
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 1);
    String capability = config.getCapability();
    capabilityStatusStore.checkAllEnabled(Collections.singleton(capability));

    //disable capability. Program should stop, status should be disabled and app should still be present.
    CapabilityConfig disabledConfig = changeConfigStatus(config, CapabilityStatus.DISABLED);
    writeConfigAsFile(externalConfigPath, disabledConfig.getCapability(), disabledConfig);
    capabilityManagementService.runTask();
    assertProgramRuns(programId, ProgramRunStatus.KILLED, 1);
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 0);
    try {
      capabilityStatusStore.checkAllEnabled(Collections.singleton(capability));
      Assert.fail("expecting exception");
    } catch (CapabilityNotAvailableException ex) {

    }
    appList = getAppList(namespace);
    Assert.assertFalse(appList.isEmpty());

    //delete capability. Program should stop, status should be disabled and app should still be present.
    new File(externalConfigPath, disabledConfig.getCapability()).delete();
    capabilityManagementService.runTask();
    Assert.assertTrue(capabilityStatusStore.getConfigs(Collections.singleton(capability)).isEmpty());
    appList = getAppList(namespace);
    Assert.assertTrue(appList.isEmpty());
  }

  @Test
  public void testCapabilityRefreshWithNewArtifact() throws Exception {
    String externalConfigPath = tmpFolder.newFolder("capability-config-refresh-redeploy").getAbsolutePath();
    cConfiguration.set(Constants.Capability.CONFIG_DIR, externalConfigPath);

    String appName = AllProgramsApp.NAME;
    String programName = AllProgramsApp.NoOpService.NAME;
    Class<AllProgramsApp> appClass = AllProgramsApp.class;
    String appVersion = "1.0.0";
    String namespace = NamespaceId.SYSTEM.getNamespace();
    //deploy the artifact with older version.
    String oldArtifactVersion = "1.0.0";
    deployTestArtifact(namespace, appName, oldArtifactVersion, appClass);

    ApplicationId applicationId = new ApplicationId(namespace, appName, appVersion);
    ProgramId programId = new ProgramId(applicationId, ProgramType.SERVICE, programName);
    //check that app is not available
    List<JsonObject> appList = getAppList(namespace);
    Assert.assertTrue(appList.isEmpty());

    //enable the capability
    CapabilityConfig config = getTestConfig(oldArtifactVersion);
    writeConfigAsFile(externalConfigPath, config.getCapability(), config);
    capabilityManagementService.runTask();
    //app should show up and program should have run
    appList = getAppList(namespace);
    Assert.assertFalse(appList.isEmpty());
    // Capability management service might not yet have deployed application.
    // So wait till program exists and is in running state.
    waitState(programId, "RUNNING");
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 1);
    String capability = config.getCapability();
    capabilityStatusStore.checkAllEnabled(Collections.singleton(capability));

    //disable capability. Program should stop, status should be disabled and app should still be present.
    CapabilityConfig disabledConfig = changeConfigStatus(config, CapabilityStatus.DISABLED);
    writeConfigAsFile(externalConfigPath, disabledConfig.getCapability(), disabledConfig);
    capabilityManagementService.runTask();
    assertProgramRuns(programId, ProgramRunStatus.KILLED, 1);
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 0);
    try {
      capabilityStatusStore.checkAllEnabled(Collections.singleton(capability));
      Assert.fail("expecting exception");
    } catch (CapabilityNotAvailableException ex) {

    }
    appList = getAppList(namespace);
    Assert.assertFalse(appList.isEmpty());

    String newArtifactVersion = "2.0.0";
    //deploy the new artifact.
    deployTestArtifact(namespace, appName, newArtifactVersion, appClass);
    //enable the capability again.
    config = getTestConfig(newArtifactVersion);
    writeConfigAsFile(externalConfigPath, config.getCapability(), config);
    capabilityManagementService.runTask();
    //app should show up with newer artifact and program should have run.
    appList = getAppList(namespace);
    Assert.assertFalse(appList.isEmpty());
    // Verify that application is redeployed with newer artifact.
    Assert.assertEquals(newArtifactVersion,
                        appList.get(0).get("artifact").getAsJsonObject().get("version").getAsString());
    // Capability management service might not yet have deployed application.
    // So wait till program exists and is in running state.
    waitState(programId, "RUNNING");
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 1);
    capability = config.getCapability();
    capabilityStatusStore.checkAllEnabled(Collections.singleton(capability));

    //disable capability again. Program should stop, status should be disabled and app should still be present.
    disabledConfig = changeConfigStatus(config, CapabilityStatus.DISABLED);
    writeConfigAsFile(externalConfigPath, disabledConfig.getCapability(), disabledConfig);
    capabilityManagementService.runTask();
    assertProgramRuns(programId, ProgramRunStatus.KILLED, 1);
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 0);
    try {
      capabilityStatusStore.checkAllEnabled(Collections.singleton(capability));
      Assert.fail("expecting exception");
    } catch (CapabilityNotAvailableException ex) {

    }
    appList = getAppList(namespace);
    Assert.assertFalse(appList.isEmpty());

    //delete capability. Program should stop, status should be disabled and app should still be present.
    new File(externalConfigPath, disabledConfig.getCapability()).delete();
    capabilityManagementService.runTask();
    Assert.assertTrue(capabilityStatusStore.getConfigs(Collections.singleton(capability)).isEmpty());
    appList = getAppList(namespace);
    Assert.assertTrue(appList.isEmpty());
  }

  @Test
  public void testCapabilityRefreshWithSameArtifact() throws Exception {
    String externalConfigPath = tmpFolder.newFolder("capability-config-refresh-redeploy-same").getAbsolutePath();
    cConfiguration.set(Constants.Capability.CONFIG_DIR, externalConfigPath);

    String appName = AllProgramsApp.NAME;
    String programName = AllProgramsApp.NoOpService.NAME;
    Class<AllProgramsApp> appClass = AllProgramsApp.class;
    String appVersion = "1.0.0";
    String namespace = NamespaceId.SYSTEM.getNamespace();
    //deploy the artifact with older version.
    String artifactVersion = "1.0.0";
    deployTestArtifact(namespace, appName, artifactVersion, appClass);

    ApplicationId applicationId = new ApplicationId(namespace, appName, appVersion);
    ProgramId programId = new ProgramId(applicationId, ProgramType.SERVICE, programName);
    //check that app is not available
    List<JsonObject> appList = getAppList(namespace);
    Assert.assertTrue(appList.isEmpty());

    //enable the capability
    CapabilityConfig config = getTestConfig(artifactVersion);
    writeConfigAsFile(externalConfigPath, config.getCapability(), config);
    capabilityManagementService.runTask();
    //app should show up and program should have run
    appList = getAppList(namespace);
    Assert.assertFalse(appList.isEmpty());
    // Capability management service might not yet have deployed application.
    // So wait till program exists and is in running state.
    waitState(programId, "RUNNING");
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 1);
    String capability = config.getCapability();
    capabilityStatusStore.checkAllEnabled(Collections.singleton(capability));

    //disable capability. Program should stop, status should be disabled and app should still be present.
    CapabilityConfig disabledConfig = changeConfigStatus(config, CapabilityStatus.DISABLED);
    writeConfigAsFile(externalConfigPath, disabledConfig.getCapability(), disabledConfig);
    capabilityManagementService.runTask();
    assertProgramRuns(programId, ProgramRunStatus.KILLED, 1);
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 0);
    try {
      capabilityStatusStore.checkAllEnabled(Collections.singleton(capability));
      Assert.fail("expecting exception");
    } catch (CapabilityNotAvailableException ex) {

    }
    appList = getAppList(namespace);
    Assert.assertFalse(appList.isEmpty());

    //enable the capability again but with same artifact.
    config = getTestConfig(artifactVersion);
    writeConfigAsFile(externalConfigPath, config.getCapability(), config);
    capabilityManagementService.runTask();
    //app should show up with newer artifact and program should have run.
    appList = getAppList(namespace);
    Assert.assertFalse(appList.isEmpty());
    // Verify that application is still same as before.
    Assert.assertEquals(artifactVersion,
                        appList.get(0).get("artifact").getAsJsonObject().get("version").getAsString());
    // Capability management service might not yet have deployed application.
    // So wait till program exists and is in running state.
    waitState(programId, "RUNNING");
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 1);
    capability = config.getCapability();
    capabilityStatusStore.checkAllEnabled(Collections.singleton(capability));

    //disable capability again. Program should stop, status should be disabled and app should still be present.
    disabledConfig = changeConfigStatus(config, CapabilityStatus.DISABLED);
    writeConfigAsFile(externalConfigPath, disabledConfig.getCapability(), disabledConfig);
    capabilityManagementService.runTask();
    assertProgramRuns(programId, ProgramRunStatus.KILLED, 1);
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 0);
    try {
      capabilityStatusStore.checkAllEnabled(Collections.singleton(capability));
      Assert.fail("expecting exception");
    } catch (CapabilityNotAvailableException ex) {

    }
    appList = getAppList(namespace);
    Assert.assertFalse(appList.isEmpty());

    //delete capability. Program should stop, status should be disabled and app should still be present.
    new File(externalConfigPath, disabledConfig.getCapability()).delete();
    capabilityManagementService.runTask();
    Assert.assertTrue(capabilityStatusStore.getConfigs(Collections.singleton(capability)).isEmpty());
    appList = getAppList(namespace);
    Assert.assertTrue(appList.isEmpty());
  }

  @Test
  public void testApplicationDeployment() throws Exception {
    String externalConfigPath = tmpFolder.newFolder("capability-config-app").getAbsolutePath();
    cConfiguration.set(Constants.Capability.CONFIG_DIR, externalConfigPath);
    capabilityManagementService.runTask();
    String testVersion = "1.0.0";
    //Deploy application with capability
    Class<CapabilityAppWithWorkflow> appWithWorkflowClass = CapabilityAppWithWorkflow.class;
    Requirements declaredAnnotation = appWithWorkflowClass.getDeclaredAnnotation(Requirements.class);
    //verify this app has capabilities
    Assert.assertTrue(declaredAnnotation.capabilities().length > 0);
    try {
      capabilityStatusStore.checkAllEnabled(Arrays.asList(declaredAnnotation.capabilities()));
      Assert.fail("expecting exception");
    } catch (CapabilityNotAvailableException ex) {

    }
    String appNameWithCapability = appWithWorkflowClass.getSimpleName() + UUID.randomUUID();
    deployTestArtifact(Id.Namespace.DEFAULT.getId(), appNameWithCapability, testVersion, appWithWorkflowClass);
    try {
      //deploy app
      Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, appNameWithCapability, testVersion);
      applicationLifecycleService
        .deployApp(NamespaceId.DEFAULT, appNameWithCapability, testVersion, artifactId,
                   null, programId -> {
          });
      Assert.fail("Expecting exception");
    } catch (CapabilityNotAvailableException ex) {
      //expected
    }

    //Deploy application without capability
    Class<WorkflowAppWithFork> appNoCapabilityClass = WorkflowAppWithFork.class;
    Requirements declaredAnnotation1 = appNoCapabilityClass.getDeclaredAnnotation(Requirements.class);
    //verify this app has no capabilities
    Assert.assertNull(declaredAnnotation1);
    String appNameWithOutCapability = appWithWorkflowClass.getSimpleName() + UUID.randomUUID();
    deployArtifactAndApp(appNoCapabilityClass, appNameWithOutCapability, testVersion);

    //enable the capabilities
    List<CapabilityConfig> capabilityConfigs = Arrays.stream(declaredAnnotation.capabilities())
      .map(capability -> new CapabilityConfig("Test capability", CapabilityStatus.ENABLED, capability,
                                              Collections.emptyList(), Collections.emptyList(),
                                              Collections.emptyList()))
      .collect(Collectors.toList());
    for (CapabilityConfig capabilityConfig : capabilityConfigs) {
      writeConfigAsFile(externalConfigPath, capabilityConfig.getCapability(), capabilityConfig);
    }
    capabilityManagementService.runTask();

    //deployment should go through now
    Id.Artifact artifactId = Id.Artifact
      .from(Id.Namespace.DEFAULT, appNameWithCapability, testVersion);
    applicationLifecycleService
      .deployApp(NamespaceId.DEFAULT, appNameWithCapability, testVersion, artifactId,
                 null, programId -> {
        });

    applicationLifecycleService.removeApplication(NamespaceId.DEFAULT.app(appNameWithCapability, testVersion));
    applicationLifecycleService.removeApplication(NamespaceId.DEFAULT.app(appNameWithOutCapability, testVersion));
    artifactRepository.deleteArtifact(Id.Artifact
                                        .from(new Id.Namespace(NamespaceId.DEFAULT.getNamespace()),
                                              appNameWithCapability, testVersion));
    artifactRepository.deleteArtifact(Id.Artifact
                                        .from(new Id.Namespace(NamespaceId.DEFAULT.getNamespace()),
                                              appNameWithOutCapability, testVersion));

    for (CapabilityConfig capabilityConfig : capabilityConfigs) {
      new File(externalConfigPath, capabilityConfig.getCapability()).delete();
    }
    capabilityManagementService.runTask();
  }

  @Test
  public void testProgramStart() throws Exception {
    String externalConfigPath = tmpFolder.newFolder("capability-config-program").getAbsolutePath();
    cConfiguration.set(Constants.Capability.CONFIG_DIR, externalConfigPath);
    String appName = CapabilitySleepingWorkflowApp.NAME;
    Class<CapabilitySleepingWorkflowApp> appClass = CapabilitySleepingWorkflowApp.class;
    String version = "1.0.0";
    String namespace = "default";
    //deploy the artifact
    deployTestArtifact(namespace, appName, version, appClass);

    //enable a capability with no system apps and programs
    CapabilityConfig enabledConfig = new CapabilityConfig("Enable healthcare", CapabilityStatus.ENABLED,
                                                          "healthcare", Collections.emptyList(),
                                                          Collections.emptyList(), Collections.emptyList());
    writeConfigAsFile(externalConfigPath, enabledConfig.getCapability(), enabledConfig);
    capabilityManagementService.runTask();
    String capability = enabledConfig.getCapability();
    capabilityStatusStore.checkAllEnabled(Collections.singleton(capability));

    //deploy an app with this capability and start a workflow
    ApplicationId applicationId = new ApplicationId(namespace, appName);
    Id.Artifact artifactId = Id.Artifact
      .from(new Id.Namespace(namespace), appName, version);
    ApplicationWithPrograms applicationWithPrograms = applicationLifecycleService
      .deployApp(new NamespaceId(namespace), appName, null, artifactId, null, op -> {
      });
    Iterable<ProgramDescriptor> programs = applicationWithPrograms.getPrograms();
    for (ProgramDescriptor program : programs) {
      programLifecycleService.start(program.getProgramId(), new HashMap<>(), false, false);
    }
    ProgramId programId = new ProgramId(applicationId, ProgramType.WORKFLOW,
                                        CapabilitySleepingWorkflowApp.SleepWorkflow.class.getSimpleName());
    // Capability management service might not yet have deployed application.
    // So wait till program exists and is in running state.
    waitState(programId, "RUNNING");
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 1);

    //disable the capability -  the program that was started should stop
    CapabilityConfig disabledConfig = new CapabilityConfig("Disable healthcare", CapabilityStatus.DISABLED,
                                                           "healthcare", Collections.emptyList(),
                                                           Collections.emptyList(), Collections.emptyList());
    writeConfigAsFile(externalConfigPath, capability, disabledConfig);
    capabilityManagementService.runTask();
    assertProgramRuns(programId, ProgramRunStatus.KILLED, 1);
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 0);
    try {
      capabilityStatusStore.checkAllEnabled(Collections.singleton(capability));
      Assert.fail("expecting exception");
    } catch (CapabilityNotAvailableException ex) {

    }

    //try starting programs
    for (ProgramDescriptor program : programs) {
      try {
        programLifecycleService.start(program.getProgramId(), new HashMap<>(), false, false);
        Assert.fail("expecting exception");
      } catch (CapabilityNotAvailableException ex) {
        //expecting exception
      }
    }
    new File(externalConfigPath, capability).delete();
    capabilityManagementService.runTask();
    Assert.assertTrue(capabilityStatusStore.getConfigs(Collections.singleton(capability)).isEmpty());
  }

  @Test
  public void testProgramWithPluginStart() throws Exception {
    String externalConfigPath = tmpFolder.newFolder("capability-config-program-plugin").getAbsolutePath();
    cConfiguration.set(Constants.Capability.CONFIG_DIR, externalConfigPath);
    String appName = CapabilitySleepingWorkflowPluginApp.NAME;
    Class<CapabilitySleepingWorkflowPluginApp> appClass = CapabilitySleepingWorkflowPluginApp.class;
    String version = "1.0.0";
    String namespace = "default";
    //deploy the artifact
    deployTestArtifact(namespace, appName, version, appClass);

    //deploy the plugin artifact
    Manifest manifest = new Manifest();
    String pluginName = CapabilitySleepingWorkflowPluginApp.SimplePlugin.class.getPackage().getName();
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, pluginName);
    Location pluginJar = PluginJarHelper
      .createPluginJar(locationFactory, manifest, CapabilitySleepingWorkflowPluginApp.SimplePlugin.class);
    Id.Artifact pluginArtifactId = Id.Artifact.from(Id.Namespace.from(namespace), pluginName, version);
    File pluginJarFile = new File(tmpFolder.newFolder(),
                                  String.format("%s-%s.jar", pluginArtifactId.getName(), version));
    Locations.linkOrCopyOverwrite(pluginJar, pluginJarFile);
    pluginJar.delete();
    artifactRepository.addArtifact(pluginArtifactId, pluginJarFile);

    //enable a capability with no system apps and programs
    CapabilityConfig enabledConfig = new CapabilityConfig("Enable healthcare", CapabilityStatus.ENABLED,
                                                          "healthcare", Collections.emptyList(),
                                                          Collections.emptyList(), Collections.emptyList());
    writeConfigAsFile(externalConfigPath, enabledConfig.getCapability(), enabledConfig);
    capabilityManagementService.runTask();
    String capability = enabledConfig.getCapability();
    capabilityStatusStore.checkAllEnabled(Collections.singleton(capability));

    //deploy an app with this capability and start a workflow
    ApplicationId applicationId = new ApplicationId(namespace, appName);
    Id.Artifact artifactId = Id.Artifact
      .from(new Id.Namespace(namespace), appName, version);
    ApplicationWithPrograms applicationWithPrograms = applicationLifecycleService
      .deployApp(new NamespaceId(namespace), appName, null, artifactId, null, op -> {
      });
    Iterable<ProgramDescriptor> programs = applicationWithPrograms.getPrograms();
    for (ProgramDescriptor program : programs) {
      programLifecycleService.start(program.getProgramId(), new HashMap<>(), false, false);
    }
    ProgramId programId = new ProgramId(applicationId, ProgramType.WORKFLOW,
                                        CapabilitySleepingWorkflowPluginApp.SleepWorkflow.class.getSimpleName());
    // Capability management service might not yet have deployed application.
    // So wait till program exists and is in running state.
    waitState(programId, "RUNNING");
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 1);

    //disable the capability -  the program that was started should stop
    CapabilityConfig disabledConfig = new CapabilityConfig("Disable healthcare", CapabilityStatus.DISABLED,
                                                           "healthcare", Collections.emptyList(),
                                                           Collections.emptyList(), Collections.emptyList());
    writeConfigAsFile(externalConfigPath, capability, disabledConfig);
    capabilityManagementService.runTask();
    assertProgramRuns(programId, ProgramRunStatus.KILLED, 1);
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 0);
    try {
      capabilityStatusStore.checkAllEnabled(Collections.singleton(capability));
      Assert.fail("expecting exception");
    } catch (CapabilityNotAvailableException ex) {

    }

    //try starting programs
    for (ProgramDescriptor program : programs) {
      try {
        programLifecycleService.start(program.getProgramId(), new HashMap<>(), false, false);
        Assert.fail("expecting exception");
      } catch (CapabilityNotAvailableException ex) {
        //expecting exception
      }
    }
    new File(externalConfigPath, capability).delete();
    capabilityManagementService.runTask();
    Assert.assertTrue(capabilityStatusStore.getConfigs(Collections.singleton(capability)).isEmpty());
  }

  @Test
  public void testStatusCheck() throws IOException {
    String testCapability1 = "cap1";
    String testCapability2 = "cap2";
    String testCapability3 = "cap3";
    capabilityStatusStore.addOrUpdateCapability(testCapability1, CapabilityStatus.ENABLED, getTestConfig(
      "1.0.0"));
    capabilityStatusStore.addOrUpdateCapability(testCapability2, CapabilityStatus.DISABLED, getTestConfig(
      "1.0.0"));
    try {
      capabilityStatusStore.checkAllEnabled(Collections.singleton(testCapability2));
      Assert.fail("expecting exception");
    } catch (CapabilityNotAvailableException ex) {
    }
    try {
      capabilityStatusStore.checkAllEnabled(Collections.singleton(testCapability3));
      Assert.fail("expecting exception");
    } catch (CapabilityNotAvailableException ex) {
    }
    try {
      capabilityStatusStore.checkAllEnabled(Collections.singleton(testCapability1));
    } catch (CapabilityNotAvailableException capabilityNotAvailableException) {
      Assert.fail("No exception expected");
    }
    try {
      capabilityStatusStore.checkAllEnabled(Collections.singleton(testCapability2));
    } catch (CapabilityNotAvailableException capabilityNotAvailableException) {
      Assert.assertEquals(new CapabilityNotAvailableException(testCapability2).getMessage(),
                          capabilityNotAvailableException.getMessage());
    }
    try {
      capabilityStatusStore.checkAllEnabled(Arrays.asList(testCapability1, testCapability2));
    } catch (CapabilityNotAvailableException capabilityNotAvailableException) {
      Assert.assertEquals(new CapabilityNotAvailableException(testCapability2).getMessage(),
                          capabilityNotAvailableException.getMessage());
    }

    try {
      capabilityStatusStore.checkAllEnabled(Arrays.asList(testCapability1, testCapability3));
    } catch (CapabilityNotAvailableException capabilityNotAvailableException) {
      Assert.assertEquals(new CapabilityNotAvailableException(testCapability3).getMessage(),
                          capabilityNotAvailableException.getMessage());
    }
    //cleanup
    capabilityStatusStore.deleteCapability(testCapability1);
    capabilityStatusStore.deleteCapability(testCapability2);
  }

  @Test
  public void testGetConfigs() throws IOException {
    String testCapability1 = "cap1";
    String testCapability2 = "cap2";
    capabilityStatusStore.addOrUpdateCapability(testCapability1, CapabilityStatus.ENABLED, getTestConfig(
      "1.0.0"));
    capabilityStatusStore.addOrUpdateCapability(testCapability2, CapabilityStatus.DISABLED, getTestConfig(
      "1.0.0"));
    Map<String, CapabilityConfig> configs = capabilityStatusStore
      .getConfigs(Arrays.asList(new String[]{testCapability1, testCapability2}));
    Assert.assertNotNull(configs.get(testCapability1));
    Assert.assertNotNull(configs.get(testCapability2));
    //cleanup
    capabilityStatusStore.deleteCapability(testCapability1);
    capabilityStatusStore.deleteCapability(testCapability2);
  }

  @Test
  public void testGetCapabilityRecords() throws IOException {
    String testCapability1 = "cap1";
    String testCapability2 = "cap2";
    String testCapability3 = "cap3";
    capabilityStatusStore.addOrUpdateCapability(testCapability1, CapabilityStatus.ENABLED, getTestConfig(
      "1.0.0"));
    capabilityStatusStore.addOrUpdateCapability(testCapability2, CapabilityStatus.DISABLED, getTestConfig(
      "1.0.0"));
    capabilityStatusStore.addOrUpdateCapabilityOperation(testCapability1, CapabilityAction.ENABLE, getTestConfig(
      "1.0.0"));
    capabilityStatusStore.addOrUpdateCapabilityOperation(testCapability3, CapabilityAction.DELETE, getTestConfig(
      "1.0.0"));
    Map<String, CapabilityRecord> capabilityRecords = capabilityStatusStore.getCapabilityRecords();
    CapabilityRecord capabilityRecord = capabilityRecords.get(testCapability1);
    Assert.assertNotNull(capabilityRecord.getCapabilityOperationRecord());
    Assert.assertNotNull(capabilityRecord.getCapabilityStatusRecord());
    capabilityRecord = capabilityRecords.get(testCapability2);
    Assert.assertNull(capabilityRecord.getCapabilityOperationRecord());
    Assert.assertNotNull(capabilityRecord.getCapabilityStatusRecord());
    capabilityRecord = capabilityRecords.get(testCapability3);
    Assert.assertNotNull(capabilityRecord.getCapabilityOperationRecord());
    Assert.assertNull(capabilityRecord.getCapabilityStatusRecord());
    //cleanup
    capabilityStatusStore.deleteCapability(testCapability1);
    capabilityStatusStore.deleteCapability(testCapability2);
    capabilityStatusStore.deleteCapabilityOperation(testCapability1);
    capabilityStatusStore.deleteCapabilityOperation(testCapability3);
  }

  private CapabilityConfig changeConfigStatus(CapabilityConfig original, CapabilityStatus status) {
    return new CapabilityConfig(original.getLabel(), status, original.getCapability(), original.getApplications(),
                                original.getPrograms(), original.getHubs());
  }

  private CapabilityConfig getTestConfig(String artifactVersion) {
    String appName = AllProgramsApp.NAME;
    String programName = AllProgramsApp.NoOpService.NAME;
    String appVersion = "1.0.0";
    String namespace = NamespaceId.SYSTEM.getNamespace();
    String label = "Enable capability";
    String capability = "test";
    ArtifactSummary artifactSummary = new ArtifactSummary(appName, artifactVersion, ArtifactScope.SYSTEM);
    SystemApplication application = new SystemApplication(namespace, appName, appVersion, artifactSummary, null);
    SystemProgram program = new SystemProgram(namespace, appName, ProgramType.SERVICE.name(),
                                              programName, appVersion, null);
    return new CapabilityConfig(label, CapabilityStatus.ENABLED, capability,
                                Collections.singletonList(application), Collections.singletonList(program),
                                Collections.emptyList());
  }

  void deployTestArtifact(String namespace, String appName, String version, Class<?> appClass) throws Exception {
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.from(namespace), appName, version);
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, appClass);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Locations.linkOrCopyOverwrite(appJar, appJarFile);
    appJar.delete();
    artifactRepository.addArtifact(artifactId, appJarFile);
  }

  private void deployArtifactAndApp(Class<?> applicationClass, String appName, String testVersion) throws Exception {
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, appName, testVersion);
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, applicationClass);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Locations.linkOrCopyOverwrite(appJar, appJarFile);
    appJar.delete();
    artifactRepository.addArtifact(artifactId, appJarFile);
    //deploy app
    applicationLifecycleService
      .deployApp(NamespaceId.DEFAULT, appName, testVersion, artifactId,
                 null, programId -> {
        });
  }
}
