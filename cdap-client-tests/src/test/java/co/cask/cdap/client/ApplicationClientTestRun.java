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
import co.cask.cdap.client.app.FakeApp;
import co.cask.cdap.client.app.FakeDatasetModule;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.common.DatasetModuleNotFoundException;
import co.cask.cdap.common.DatasetNotFoundException;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.test.XSlowTests;
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

    try {
      datasetClient.delete(FakeApp.DS_NAME);
    } catch (DatasetNotFoundException e) {
      // NO-OP
    }

    try {
      datasetModuleClient.delete(FakeDatasetModule.NAME);
    } catch (DatasetModuleNotFoundException e) {
      // NO-OP
    }
  }

  @Test
  public void testAll() throws Exception {
    Assert.assertEquals(0, appClient.list().size());

    // deploy app
    LOG.info("Deploying app");
    appClient.deploy(createAppJarFile(FakeApp.class));
    appClient.waitForDeployed(FakeApp.NAME, 30, TimeUnit.SECONDS);
    Assert.assertEquals(1, appClient.list().size());

    try {
      // check program list
      LOG.info("Checking program list for app");
      Map<ProgramType, List<ProgramRecord>> programs = appClient.listProgramsByType(FakeApp.NAME);
      verifyProgramNames(FakeApp.FLOWS, programs.get(ProgramType.FLOW));
      verifyProgramNames(FakeApp.MAPREDUCES, programs.get(ProgramType.MAPREDUCE));
      verifyProgramNames(FakeApp.WORKFLOWS, programs.get(ProgramType.WORKFLOW));
      verifyProgramNames(FakeApp.SERVICES, programs.get(ProgramType.SERVICE));

      verifyProgramNames(FakeApp.FLOWS, appClient.listPrograms(FakeApp.NAME, ProgramType.FLOW));
      verifyProgramNames(FakeApp.MAPREDUCES, appClient.listPrograms(FakeApp.NAME, ProgramType.MAPREDUCE));
      verifyProgramNames(FakeApp.WORKFLOWS, appClient.listPrograms(FakeApp.NAME, ProgramType.WORKFLOW));
      verifyProgramNames(FakeApp.SERVICES, appClient.listPrograms(FakeApp.NAME, ProgramType.SERVICE));

      verifyProgramNames(FakeApp.FLOWS, appClient.listAllPrograms(ProgramType.FLOW));
      verifyProgramNames(FakeApp.MAPREDUCES, appClient.listAllPrograms(ProgramType.MAPREDUCE));
      verifyProgramNames(FakeApp.WORKFLOWS, appClient.listAllPrograms(ProgramType.WORKFLOW));
      verifyProgramNames(FakeApp.SERVICES, appClient.listAllPrograms(ProgramType.SERVICE));

      verifyProgramRecords(FakeApp.ALL_PROGRAMS, appClient.listAllPrograms());
    } finally {
      // delete app
      LOG.info("Deleting app");
      appClient.delete(FakeApp.NAME);
      appClient.waitForDeleted(FakeApp.NAME, 30, TimeUnit.SECONDS);
      Assert.assertEquals(0, appClient.list().size());
    }
  }

  @Test
  public void testDeleteAll() throws Exception {
    Assert.assertEquals(0, appClient.list().size());

    try {
      // deploy first app
      LOG.info("Deploying first app");
      appClient.deploy(createAppJarFile(FakeApp.class));
      appClient.waitForDeployed(FakeApp.NAME, 30, TimeUnit.SECONDS);
      Assert.assertEquals(1, appClient.list().size());

      // deploy second app
      LOG.info("Deploying second app");
      appClient.deploy(createAppJarFile(AppReturnsArgs.class));
      appClient.waitForDeployed(AppReturnsArgs.NAME, 30, TimeUnit.SECONDS);
      Assert.assertEquals(2, appClient.list().size());
    } finally {
      appClient.deleteAll();
      appClient.waitForDeleted(FakeApp.NAME, 30, TimeUnit.SECONDS);
      appClient.waitForDeleted(AppReturnsArgs.NAME, 30, TimeUnit.SECONDS);
      Assert.assertEquals(0, appClient.list().size());
    }
  }
}
