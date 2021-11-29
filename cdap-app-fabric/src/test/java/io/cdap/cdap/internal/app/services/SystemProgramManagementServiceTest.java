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

package io.cdap.cdap.internal.app.services;

import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.ProgramRuntimeService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Test for SystemProgramManagementService
 */
public class SystemProgramManagementServiceTest extends AppFabricTestBase {

  private static SystemProgramManagementService progmMgmtSvc;
  private static ProgramLifecycleService programLifecycleService;
  private static ApplicationLifecycleService applicationLifecycleService;
  private static LocationFactory locationFactory;
  private static ArtifactRepository artifactRepository;

  private static final String VERSION = "1.0.0";
  private static final String APP_NAME = SystemProgramManagementTestApp.NAME;
  private static final String NAMESPACE = "system";
  private static final String PROGRAM_NAME = SystemProgramManagementTestApp.IdleWorkflow.NAME;
  private static final Class<?> APP_CLASS = SystemProgramManagementTestApp.class;

  @BeforeClass
  public static void setup() {
    programLifecycleService = getInjector().getInstance(ProgramLifecycleService.class);
    applicationLifecycleService = getInjector().getInstance(ApplicationLifecycleService.class);
    locationFactory = getInjector().getInstance(LocationFactory.class);
    artifactRepository = getInjector().getInstance(ArtifactRepository.class);
    progmMgmtSvc = new SystemProgramManagementService(getInjector().getInstance(CConfiguration.class),
                                                      getInjector().getInstance(ProgramRuntimeService.class),
                                                      programLifecycleService);
    progmMgmtSvc.stopAndWait();
  }

  @AfterClass
  public static void stop() {
    AppFabricTestHelper.shutdown();
  }

  @Test
  public void testIteration() throws Exception {
    //deploy app to test as a system service
    deployTestApp();
    //Set this app as an enabled service
    ApplicationId applicationId = new ApplicationId(NAMESPACE, APP_NAME, VERSION);
    ProgramId programId = new ProgramId(applicationId, ProgramType.WORKFLOW, PROGRAM_NAME);
    Map<ProgramId, Arguments> enabledServices = new HashMap<>();
    enabledServices.put(programId, new BasicArguments(new HashMap<>()));
    progmMgmtSvc.setProgramsEnabled(enabledServices);
    //run one iteration of programManagementService. The program should start
    progmMgmtSvc.runTask();
    waitState(programId, ProgramStatus.RUNNING.name());
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 1);
    //Remove this app as an enabled service , program should stop
    progmMgmtSvc.setProgramsEnabled(new HashMap<>());
    progmMgmtSvc.runTask();
    waitState(programId, ProgramStatus.STOPPED.name());
    Assert.assertEquals(ProgramStatus.STOPPED.name(), getProgramStatus(programId));
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 0);
    //Run the program manually twice to test pruning. One run should be killed
    programLifecycleService.start(programId, new HashMap<>(), false, false);
    programLifecycleService.start(programId, new HashMap<>(), false, false);
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 2);
    progmMgmtSvc.setProgramsEnabled(enabledServices);
    progmMgmtSvc.runTask();
    assertProgramRuns(programId, ProgramRunStatus.KILLED, 2);
    assertProgramRuns(programId, ProgramRunStatus.RUNNING, 1);
  }

  private void deployTestApp() throws Exception {
    Id.Artifact artifactId = Id.Artifact
      .from(Id.Namespace.SYSTEM, APP_NAME, VERSION);
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, APP_CLASS);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Locations.linkOrCopyOverwrite(appJar, appJarFile);
    appJar.delete();
    artifactRepository.addArtifact(artifactId, appJarFile);
    ArtifactSummary summary = new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion(),
                                                  ArtifactScope.SYSTEM);
    applicationLifecycleService.deployApp(NamespaceId.SYSTEM, APP_NAME, VERSION, summary, null,
                                          programId -> {
                                            // no-op
                                          }, null, false, false, Collections.emptyMap());
  }
}
