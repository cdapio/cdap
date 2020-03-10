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

package io.cdap.cdap.internal.sysapp;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.inject.Injector;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import org.iq80.leveldb.util.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for the {@link SystemAppManagementService}.
 */
public class SystemAppManagementServiceTest extends AppFabricTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(SystemAppManagementServiceTest.class);
  private static final Gson GSON = new Gson();

  private static ProgramLifecycleService programLifecycleService;
  private static ApplicationLifecycleService applicationLifecycleService;
  private static CConfiguration cConf;
  private static SystemAppManagementService systemAppManagementService;
  private static File systemConfigDir;

  private static final String RUNNING = "RUNNING";

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();


  @BeforeClass
  public static void setup() throws IOException {
    Injector injector = getInjector();
    programLifecycleService = injector.getInstance(ProgramLifecycleService.class);
    applicationLifecycleService = injector.getInstance(ApplicationLifecycleService.class);
    cConf = injector.getInstance(CConfiguration.class);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    systemAppManagementService.shutDown();
  }

  private void createEnableSysAppConfigFile(Id.Artifact artifactId, String filename) throws IOException {
    AppRequest<JsonObject> appRequest = new AppRequest<>(new ArtifactSummary(artifactId.getName(),
                                                         artifactId.getVersion().getVersion()));
    SystemAppStep.Arguments step1Argument =
      new SystemAppStep.Arguments(appRequest, artifactId.getNamespace().getId(), artifactId.getName(), false);
    List<SystemAppStep> steps = new ArrayList<>();
    steps.add(
      new SystemAppStep("step for " + artifactId.getName(), SystemAppStep.Type.ENABLE_SYSTEM_APP, step1Argument));
    SystemAppConfig config = new SystemAppConfig(steps);
    File tmpFile = new File(systemConfigDir, filename);
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(tmpFile))) {
      bw.write(GSON.toJson(config));
    }
  }

  /**
   * Tests SystemAppManagementService end to end by running below scenario:
   * 1. Creates a system app config for an application into corresponding directory.
   * 2. Successfully read and load the config.
   * 3. Runs all steps to enable a system app , tests SystemAppEnableExecutor.
   * 4. Deploys the app.
   * 5. Runs all programs corresponding to the app.
   * 6. Checks status of a continuously running program, i.e a service program.
   * @throws Exception
   */
  @Test
  public void testSystemAppManagementServiceE2E() throws Exception {
    systemConfigDir = TEMPORARY_FOLDER.newFolder("demo-sys-app-config-dir");
    cConf.set(Constants.SYSTEM_APP_CONFIG_DIR, systemConfigDir.getAbsolutePath());
    systemAppManagementService = new SystemAppManagementService(cConf, applicationLifecycleService,
                                                                programLifecycleService);
    Id.Artifact artifactId1 = Id.Artifact.from(Id.Namespace.DEFAULT, "App", VERSION1);
    addAppArtifact(artifactId1, AllProgramsApp.class);
    createEnableSysAppConfigFile(artifactId1, "demo.json");
    systemAppManagementService.startUp();
    ApplicationId appId1 = NamespaceId.DEFAULT.app(AllProgramsApp.NAME);
    ProgramId serviceId1 = appId1.program(ProgramType.SERVICE, AllProgramsApp.NoOpService.NAME);
    waitState(serviceId1, RUNNING);
    Assert.assertEquals(RUNNING, getProgramStatus(serviceId1));
  }
}
