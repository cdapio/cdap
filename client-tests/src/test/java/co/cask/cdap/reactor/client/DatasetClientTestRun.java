/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.reactor.client;

import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.client.DatasetModuleClient;
import co.cask.cdap.client.DatasetTypeClient;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.reactor.client.app.StandaloneDataset;
import co.cask.cdap.reactor.client.app.StandaloneDatasetModule;
import co.cask.cdap.reactor.client.common.ClientTestBase;
import co.cask.cdap.test.XSlowTests;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Test for {@link DatasetClient}, {@link DatasetModuleClient}, and {@link DatasetTypeClient}.
 */
@Category(XSlowTests.class)
public class DatasetClientTestRun extends ClientTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetClientTestRun.class);

  private DatasetClient datasetClient;
  private DatasetModuleClient moduleClient;
  private DatasetTypeClient typeClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    datasetClient = new DatasetClient(clientConfig);
    moduleClient = new DatasetModuleClient(clientConfig);
    typeClient = new DatasetTypeClient(clientConfig);
  }

  @Test
  public void testAll() throws Exception {
    int numBaseModules = moduleClient.list().size();
    int numBaseTypes = typeClient.list().size();

    LOG.info("Adding Dataset module");
    File moduleJarFile = createAppJarFile(StandaloneDatasetModule.class);
    moduleClient.add(StandaloneDatasetModule.NAME, StandaloneDatasetModule.class.getName(), moduleJarFile);
    Assert.assertEquals(numBaseModules + 1, moduleClient.list().size());
    Assert.assertEquals(numBaseTypes + 2, typeClient.list().size());

    LOG.info("Checking that the new Dataset module exists");
    DatasetModuleMeta datasetModuleMeta = moduleClient.get(StandaloneDatasetModule.NAME);
    Assert.assertNotNull(datasetModuleMeta);
    Assert.assertEquals(StandaloneDatasetModule.NAME, datasetModuleMeta.getName());

    LOG.info("Checking that the new Dataset type exists");
    DatasetTypeMeta datasetTypeMeta = typeClient.get(StandaloneDataset.TYPE_NAME);
    Assert.assertNotNull(datasetTypeMeta);
    Assert.assertEquals(StandaloneDataset.TYPE_NAME, datasetTypeMeta.getName());

    datasetTypeMeta = typeClient.get(StandaloneDataset.class.getName());
    Assert.assertNotNull(datasetTypeMeta);
    Assert.assertEquals(StandaloneDataset.class.getName(), datasetTypeMeta.getName());

    LOG.info("Creating, truncating, and deleting dataset of new Dataset type");
    // Before creating dataset, there are some system datasets already exist
    int numBaseDataset = datasetClient.list().size();

    datasetClient.create("testDataset", StandaloneDataset.TYPE_NAME);
    Assert.assertEquals(numBaseDataset + 1, datasetClient.list().size());
    datasetClient.truncate("testDataset");
    datasetClient.delete("testDataset");
    Assert.assertEquals(numBaseDataset, datasetClient.list().size());

    LOG.info("Creating and deleting multiple Datasets");
    for (int i = 1; i <= 3; i++) {
      datasetClient.create("testDataset" + i, StandaloneDataset.TYPE_NAME);
    }
    Assert.assertEquals(numBaseDataset + 3, datasetClient.list().size());
    for (int i = 1; i <= 3; i++) {
      datasetClient.delete("testDataset" + i);
    }
    Assert.assertEquals(numBaseDataset, datasetClient.list().size());

    LOG.info("Deleting Dataset module");
    moduleClient.delete(StandaloneDatasetModule.NAME);
    Assert.assertEquals(numBaseModules, moduleClient.list().size());
    Assert.assertEquals(numBaseTypes, typeClient.list().size());

    LOG.info("Adding Dataset module and then deleting all Dataset modules");
    moduleClient.add("testModule1", StandaloneDatasetModule.class.getName(), moduleJarFile);
    Assert.assertEquals(numBaseModules + 1, moduleClient.list().size());
    Assert.assertEquals(numBaseTypes + 2, typeClient.list().size());

    moduleClient.deleteAll();
    Assert.assertEquals(numBaseModules, moduleClient.list().size());
    Assert.assertEquals(numBaseTypes, typeClient.list().size());
  }
}
