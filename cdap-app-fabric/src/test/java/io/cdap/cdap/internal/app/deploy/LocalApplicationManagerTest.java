/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.deploy;

import com.google.gson.Gson;
import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.ConfigTestApp;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
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
import java.io.FileOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Tests the functionality of Deploy Manager.
 */
public class LocalApplicationManagerTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  public static final Gson GSON = new Gson();

  private static LocationFactory lf;

  @BeforeClass
  public static void before() throws Exception {
    lf = new LocalLocationFactory(TMP_FOLDER.newFolder());

    NamespaceAdmin namespaceAdmin = AppFabricTestHelper.getInjector().getInstance(NamespaceAdmin.class);
    namespaceAdmin.create(NamespaceMeta.DEFAULT);
  }

  @AfterClass
  public static void tearDown() {
    AppFabricTestHelper.shutdown();
  }

  /**
   * Improper Manifest file should throw an exception.
   */
  @Test(expected = ExecutionException.class)
  public void testImproperOrNoManifestFile() throws Exception {
    // Create an JAR without the MainClass set.
    File deployFile = TMP_FOLDER.newFile();
    try (JarOutputStream output = new JarOutputStream(new FileOutputStream(deployFile), new Manifest())) {
      output.putNextEntry(new JarEntry("dummy"));
    }

    Location jarLoc = Locations.toLocation(deployFile);

    ArtifactId artifactId = new ArtifactId("dummy", new ArtifactVersion("1.0.0-SNAPSHOT"), ArtifactScope.USER);
    ArtifactDescriptor artifactDescriptor = new ArtifactDescriptor(artifactId, jarLoc);
    String className = "some.class.name";
    ApplicationClass applicationClass = new ApplicationClass(className, "", null);
    AppDeploymentInfo info = new AppDeploymentInfo(artifactDescriptor, NamespaceId.DEFAULT, applicationClass, null,
                                                   null, null);
    AppFabricTestHelper.getLocalManager().deploy(info).get();
  }

  /**
   * Good pipeline with good tests.
   */
  @Test
  public void testGoodPipeline() throws Exception {
    Location deployedJar = AppJarHelper.createDeploymentJar(lf, AllProgramsApp.class);
    ArtifactId artifactId = new ArtifactId("app", new ArtifactVersion("1.0.0-SNAPSHOT"), ArtifactScope.USER);
    ArtifactDescriptor artifactDescriptor = new ArtifactDescriptor(artifactId, deployedJar);
    ApplicationClass applicationClass = new ApplicationClass(AllProgramsApp.class.getName(), "", null);
    AppDeploymentInfo info = new AppDeploymentInfo(artifactDescriptor, NamespaceId.DEFAULT,
                                                   applicationClass, null, null, null);
    ApplicationWithPrograms input = AppFabricTestHelper.getLocalManager().deploy(info).get();

    ApplicationSpecification appSpec = Specifications.from(new AllProgramsApp());

    // Validate that all programs are being captured by the deployment pipeline
    Map<ProgramType, Set<String>> programByTypes = new HashMap<>();
    for (ProgramDescriptor desc : input.getPrograms()) {
      ProgramId programId = desc.getProgramId();
      programByTypes.computeIfAbsent(programId.getType(), k -> new HashSet<>()).add(programId.getProgram());
    }
    for (io.cdap.cdap.api.app.ProgramType programType : io.cdap.cdap.api.app.ProgramType.values()) {
      Assert.assertEquals(appSpec.getProgramsByType(programType),
                          programByTypes.getOrDefault(ProgramType.valueOf(programType.name()), Collections.emptySet()));
    }
  }

  @Test
  public void testValidConfigPipeline() throws Exception {
    Location deployedJar = AppJarHelper.createDeploymentJar(lf, ConfigTestApp.class);
    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("myTable");
    ArtifactId artifactId = new ArtifactId("configtest", new ArtifactVersion("1.0.0-SNAPSHOT"), ArtifactScope.USER);
    ArtifactDescriptor artifactDescriptor = new ArtifactDescriptor(artifactId, deployedJar);
    ApplicationClass applicationClass = new ApplicationClass(ConfigTestApp.class.getName(), "", null);
    AppDeploymentInfo info = new AppDeploymentInfo(artifactDescriptor, NamespaceId.DEFAULT,
                                                   applicationClass, "MyApp", null, GSON.toJson(config));
    AppFabricTestHelper.getLocalManager().deploy(info).get();
  }

  @Test(expected = ExecutionException.class)
  public void testInvalidConfigPipeline() throws Exception {
    Location deployedJar = AppJarHelper.createDeploymentJar(lf, ConfigTestApp.class);
    ArtifactId artifactId = new ArtifactId("configtest", new ArtifactVersion("1.0.0-SNAPSHOT"), ArtifactScope.USER);
    ArtifactDescriptor artifactDescriptor = new ArtifactDescriptor(artifactId, deployedJar);
    ApplicationClass applicationClass = new ApplicationClass(ConfigTestApp.class.getName(), "", null);
    AppDeploymentInfo info = new AppDeploymentInfo(artifactDescriptor, NamespaceId.DEFAULT,
                                                   applicationClass, "BadApp", null,
                                                   GSON.toJson("invalid"));
    AppFabricTestHelper.getLocalManager().deploy(info).get();
  }
}
