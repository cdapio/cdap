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

package co.cask.cdap.client;

import co.cask.cdap.client.app.AppReturnsArgs;
import co.cask.cdap.client.app.ConfigTestApp;
import co.cask.cdap.client.app.FakeApp;
import co.cask.cdap.client.app.FakeDatasetModule;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.common.DatasetModuleNotFoundException;
import co.cask.cdap.common.DatasetNotFoundException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.XSlowTests;
import com.google.gson.Gson;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link ApplicationClient}.
 */
@Category(XSlowTests.class)
public class ApplicationClientTestRun extends ClientTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationClientTestRun.class);
  private static final Gson GSON = new Gson();

  private ApplicationClient appClient;
  private DatasetClient datasetClient;
  private DatasetModuleClient datasetModuleClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    appClient = new ApplicationClient(clientConfig);
    datasetClient = new DatasetClient(clientConfig);
    datasetModuleClient = new DatasetModuleClient(clientConfig);
  }

  @After
  public void cleanup() throws Throwable {
    // Delete FakeApp's dataset and module so that DatasetClientTestRun works when running both inside a test suite
    // This is due to DatasetClientTestRun assuming that it is using a blank CDAP instance

    Id.Namespace namespace = Id.Namespace.DEFAULT;

    try {
      datasetClient.delete(Id.DatasetInstance.from(namespace, FakeApp.DS_NAME));
    } catch (DatasetNotFoundException e) {
      // NO-OP
    }

    try {
      datasetModuleClient.delete(Id.DatasetModule.from(namespace, FakeDatasetModule.NAME));
    } catch (DatasetModuleNotFoundException e) {
      // NO-OP
    }
  }

  @Test
  public void testAll() throws Exception {
    Id.Application app = Id.Application.from(Id.Namespace.DEFAULT, FakeApp.NAME);

    Assert.assertEquals(0, appClient.list(Id.Namespace.DEFAULT).size());

    // deploy app
    LOG.info("Deploying app");
    appClient.deploy(Id.Namespace.DEFAULT, createAppJarFile(FakeApp.class));
    appClient.waitForDeployed(app, 30, TimeUnit.SECONDS);
    Assert.assertEquals(1, appClient.list(Id.Namespace.DEFAULT).size());

    try {
      // check program list
      LOG.info("Checking program list for app");
      Map<ProgramType, List<ProgramRecord>> programs = appClient.listProgramsByType(app);
      verifyProgramNames(FakeApp.FLOWS, programs.get(ProgramType.FLOW));
      verifyProgramNames(FakeApp.MAPREDUCES, programs.get(ProgramType.MAPREDUCE));
      verifyProgramNames(FakeApp.WORKFLOWS, programs.get(ProgramType.WORKFLOW));
      verifyProgramNames(FakeApp.SERVICES, programs.get(ProgramType.SERVICE));

      verifyProgramNames(FakeApp.FLOWS, appClient.listPrograms(app, ProgramType.FLOW));
      verifyProgramNames(FakeApp.MAPREDUCES, appClient.listPrograms(app, ProgramType.MAPREDUCE));
      verifyProgramNames(FakeApp.WORKFLOWS, appClient.listPrograms(app, ProgramType.WORKFLOW));
      verifyProgramNames(FakeApp.SERVICES, appClient.listPrograms(app, ProgramType.SERVICE));

      verifyProgramNames(FakeApp.FLOWS, appClient.listAllPrograms(Id.Namespace.DEFAULT, ProgramType.FLOW));
      verifyProgramNames(FakeApp.MAPREDUCES, appClient.listAllPrograms(Id.Namespace.DEFAULT, ProgramType.MAPREDUCE));
      verifyProgramNames(FakeApp.WORKFLOWS, appClient.listAllPrograms(Id.Namespace.DEFAULT, ProgramType.WORKFLOW));
      verifyProgramNames(FakeApp.SERVICES, appClient.listAllPrograms(Id.Namespace.DEFAULT, ProgramType.SERVICE));

      verifyProgramRecords(FakeApp.ALL_PROGRAMS, appClient.listAllPrograms(Id.Namespace.DEFAULT));
    } finally {
      // delete app
      LOG.info("Deleting app");
      appClient.delete(app);
      appClient.waitForDeleted(app, 30, TimeUnit.SECONDS);
      Assert.assertEquals(0, appClient.list(Id.Namespace.DEFAULT).size());
    }
  }

  @Test
  public void testAppConfig() throws Exception {
    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("testStream", "testDataset");
    appClient.deploy(Id.Namespace.DEFAULT, createAppJarFile(ConfigTestApp.class), config);
    Assert.assertEquals(1, appClient.list().size());

    try {
      appClient.exists(ConfigTestApp.NAME);
    } finally {
      appClient.delete(ConfigTestApp.NAME);
      appClient.waitForDeleted(ConfigTestApp.NAME, 30, TimeUnit.SECONDS);
      Assert.assertEquals(0, appClient.list().size());
    }
  }

  @Test
  public void testDeleteAll() throws Exception {
    Id.Namespace namespace = Id.Namespace.DEFAULT;
    Id.Application app = Id.Application.from(namespace, FakeApp.NAME);
    Id.Application app2 = Id.Application.from(namespace, AppReturnsArgs.NAME);

    Assert.assertEquals(0, appClient.list(namespace).size());

    try {
      // deploy first app
      LOG.info("Deploying first app");
      appClient.deploy(namespace, createAppJarFile(FakeApp.class));
      appClient.waitForDeployed(app, 30, TimeUnit.SECONDS);
      Assert.assertEquals(1, appClient.list(namespace).size());

      // deploy second app
      LOG.info("Deploying second app");
      appClient.deploy(namespace, createAppJarFile(AppReturnsArgs.class));
      appClient.waitForDeployed(app2, 30, TimeUnit.SECONDS);
      Assert.assertEquals(2, appClient.list(namespace).size());
    } finally {
      appClient.deleteAll(namespace);
      appClient.waitForDeleted(app, 30, TimeUnit.SECONDS);
      appClient.waitForDeleted(app2, 30, TimeUnit.SECONDS);
      Assert.assertEquals(0, appClient.list(namespace).size());
    }
  }
}
