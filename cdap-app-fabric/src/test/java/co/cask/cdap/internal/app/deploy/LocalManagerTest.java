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

import co.cask.cdap.ToyApp;
import co.cask.cdap.WebCrawlApp;
import co.cask.cdap.common.lang.jar.JarFinder;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.deploy.pipeline.DeploymentInfo;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.internal.AppFabricClient;
import co.cask.cdap.test.internal.AppFabricTestHelper;
import co.cask.cdap.test.internal.DefaultId;
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
import java.util.concurrent.ExecutionException;
import java.util.jar.Manifest;

/**
 * Tests the functionality of Deploy Manager.
 */
public class LocalManagerTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static LocationFactory lf;
  private static File temp;

  @BeforeClass
  public static void before() throws Exception {
    lf = new LocalLocationFactory();
    temp = TMP_FOLDER.newFolder("pipeline");
  }

  /**
   * Improper Manifest file should throw an exception.
   */
  @Test(expected = ExecutionException.class)
  public void testImproperOrNoManifestFile() throws Exception {
    String jar = JarFinder.getJar(WebCrawlApp.class, new Manifest());
    Location deployedJar = lf.create(jar);

    Location destination = new LocalLocationFactory().create(temp.toURI());
    DeploymentInfo info = new DeploymentInfo(new File(deployedJar.toURI()), destination);

    try {
      AppFabricTestHelper.getLocalManager().deploy(DefaultId.NAMESPACE, null, info).get();
    } finally {
      deployedJar.delete(true);
    }
  }

  /**
   * Good pipeline with good tests.
   */
  @Test
  public void testGoodPipeline() throws Exception {
    // TODO modify
    Location deployedJar = lf.create(
      JarFinder.getJar(ToyApp.class, AppFabricClient.getManifestWithMainClass(ToyApp.class))
    );

    Location destination = new LocalLocationFactory().create(temp.toURI()).append("test.jar");
    DeploymentInfo info = new DeploymentInfo(new File(deployedJar.toURI()), destination);

    ApplicationWithPrograms input = AppFabricTestHelper.getLocalManager().deploy(DefaultId.NAMESPACE,
                                                                                 null, info).get();

    Assert.assertEquals(input.getPrograms().iterator().next().getType(), ProgramType.FLOW);
    Assert.assertEquals(input.getPrograms().iterator().next().getName(), "ToyFlow");
  }

  @AfterClass
  public static void cleanup() {
    TMP_FOLDER.delete();
  }

}
