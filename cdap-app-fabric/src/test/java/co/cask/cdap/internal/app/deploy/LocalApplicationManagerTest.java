/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.deploy;

import co.cask.cdap.ConfigTestApp;
import co.cask.cdap.ToyApp;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.DefaultId;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.deploy.pipeline.DeploymentInfo;
import co.cask.cdap.internal.app.namespace.NamespaceAdmin;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.ProgramType;
import com.google.gson.Gson;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
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
    namespaceAdmin.createNamespace(Constants.DEFAULT_NAMESPACE_META);
  }

  /**
   * Improper Manifest file should throw an exception.
   */
  @Test(expected = ExecutionException.class)
  public void testImproperOrNoManifestFile() throws Exception {
    // Create an JAR without the MainClass set.
    File deployFile = TMP_FOLDER.newFile();
    JarOutputStream output = new JarOutputStream(new FileOutputStream(deployFile), new Manifest());
    try {
      output.putNextEntry(new JarEntry("dummy"));
    } finally {
      output.close();
    }

    Location destination = new LocalLocationFactory().create(new File(TMP_FOLDER.newFolder(), "deploy.jar").toURI());
    DeploymentInfo info = new DeploymentInfo(deployFile, destination, null);

    AppFabricTestHelper.getLocalManager().deploy(DefaultId.NAMESPACE, null, info).get();
  }

  /**
   * Good pipeline with good tests.
   */
  @Test
  public void testGoodPipeline() throws Exception {
    Location deployedJar = AppJarHelper.createDeploymentJar(lf, ToyApp.class);
    Location destination = new LocalLocationFactory().create(new File(TMP_FOLDER.newFolder(), "deploy.jar").toURI());
    DeploymentInfo info = new DeploymentInfo(new File(deployedJar.toURI()), destination, null);

    ApplicationWithPrograms input = AppFabricTestHelper.getLocalManager().deploy(DefaultId.NAMESPACE,
                                                                                 null, info).get();

    Assert.assertEquals(input.getPrograms().iterator().next().getType(), ProgramType.FLOW);
    Assert.assertEquals(input.getPrograms().iterator().next().getName(), "ToyFlow");
  }

  @Test
  public void testValidConfigPipeline() throws Exception {
    Location deployedJar = AppJarHelper.createDeploymentJar(lf, ConfigTestApp.class);
    Location destination = new LocalLocationFactory().create(new File(TMP_FOLDER.newFolder(), "deploy.jar").toURI());

    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("myStream", "myTable");
    DeploymentInfo info = new DeploymentInfo(new File(deployedJar.toURI()), destination, GSON.toJson(config));

    AppFabricTestHelper.getLocalManager().deploy(DefaultId.NAMESPACE, "MyApp", info).get();
  }

  @Test(expected = ExecutionException.class)
  public void testInvalidConfigPipeline() throws Exception {
    Location deployedJar = AppJarHelper.createDeploymentJar(lf, ConfigTestApp.class);
    Location destination = new LocalLocationFactory().create(new File(TMP_FOLDER.newFolder(), "deploy.jar").toURI());

    DeploymentInfo info = new DeploymentInfo(new File(deployedJar.toURI()), destination, GSON.toJson("invalid"));
    AppFabricTestHelper.getLocalManager().deploy(DefaultId.NAMESPACE, "BadApp", info).get();
  }
}
