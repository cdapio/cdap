/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.client.app.StandaloneDataset;
import co.cask.cdap.client.app.StandaloneDatasetModule;
import co.cask.cdap.client.common.ClientTestBase;
import co.cask.cdap.common.exception.AlreadyExistsException;
import co.cask.cdap.common.exception.DatasetModuleNotFoundException;
import co.cask.cdap.common.exception.DatasetTypeNotFoundException;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.test.XSlowTests;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link DatasetClient}, {@link DatasetModuleClient}, and {@link DatasetTypeClient}.
 */
@Category(XSlowTests.class)
public class DatasetClientTestRun extends ClientTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetClientTestRun.class);
  private static final String TEST_NAMESPACE = "testNamespace";
  private static final String OTHER_NAMESPACE = "otherNamespace";

  private DatasetClient datasetClient;
  private DatasetModuleClient moduleClient;
  private DatasetTypeClient typeClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();
    datasetClient = new DatasetClient(clientConfig);
    moduleClient = new DatasetModuleClient(clientConfig);
    typeClient = new DatasetTypeClient(clientConfig);
    NamespaceClient namespaceClient = new NamespaceClient(clientConfig);
    try {
      namespaceClient.create(new NamespaceMeta.Builder().setId(TEST_NAMESPACE).build());
    } catch (AlreadyExistsException e) {
    }
    try {
      namespaceClient.create(new NamespaceMeta.Builder().setId(OTHER_NAMESPACE).build());
    } catch (AlreadyExistsException e) {
    }
    clientConfig.setNamespace(TEST_NAMESPACE);
  }

  @After
  public void tearDown() throws Exception {
    NamespaceClient namespaceClient = new NamespaceClient(clientConfig);
    namespaceClient.delete(TEST_NAMESPACE);
    namespaceClient.delete(OTHER_NAMESPACE);
  }

  @Test
  public void testAll() throws Exception {
    int numBaseModules = moduleClient.list().size();
    int numBaseTypes = typeClient.list().size();

    LOG.info("Adding Dataset module");
    File moduleJarFile = createAppJarFile(StandaloneDatasetModule.class);
    moduleClient.add(StandaloneDatasetModule.NAME, StandaloneDatasetModule.class.getName(), moduleJarFile);
    moduleClient.waitForExists(StandaloneDatasetModule.NAME, 30, TimeUnit.SECONDS);
    Assert.assertEquals(numBaseModules + 1, moduleClient.list().size());
    Assert.assertEquals(numBaseTypes + 2, typeClient.list().size());

    LOG.info("Checking that the new Dataset module exists");
    DatasetModuleMeta datasetModuleMeta = moduleClient.get(StandaloneDatasetModule.NAME);
    Assert.assertNotNull(datasetModuleMeta);
    Assert.assertEquals(StandaloneDatasetModule.NAME, datasetModuleMeta.getName());

    LOG.info("Checking that the new Dataset module does not exist in a different namespace");
    clientConfig.setNamespace(OTHER_NAMESPACE);
    try {
      moduleClient.get(StandaloneDatasetModule.NAME);
      Assert.fail("datasetModule found in namespace other than one in which it was expected");
    } catch (DatasetModuleNotFoundException expected) {
    }
    clientConfig.setNamespace(TEST_NAMESPACE);

    LOG.info("Checking that the new Dataset type exists");
    typeClient.waitForExists(StandaloneDataset.TYPE_NAME, 5, TimeUnit.SECONDS);
    DatasetTypeMeta datasetTypeMeta = typeClient.get(StandaloneDataset.TYPE_NAME);
    Assert.assertNotNull(datasetTypeMeta);
    Assert.assertEquals(StandaloneDataset.TYPE_NAME, datasetTypeMeta.getName());

    datasetTypeMeta = typeClient.get(StandaloneDataset.class.getName());
    Assert.assertNotNull(datasetTypeMeta);
    Assert.assertEquals(StandaloneDataset.class.getName(), datasetTypeMeta.getName());

    LOG.info("Checking that the new Dataset module does not exist in a different namespace");
    clientConfig.setNamespace(OTHER_NAMESPACE);
    try {
      typeClient.get(StandaloneDataset.class.getName());
      Assert.fail("datasetType found in namespace other than one in which it was expected");
    } catch (DatasetTypeNotFoundException expected) {
    }
    clientConfig.setNamespace(TEST_NAMESPACE);

    LOG.info("Creating, truncating, and deleting dataset of new Dataset type");
    // Before creating dataset, there are some system datasets already exist
    int numBaseDataset = datasetClient.list().size();

    datasetClient.create("testDataset", StandaloneDataset.TYPE_NAME);
    Assert.assertEquals(numBaseDataset + 1, datasetClient.list().size());
    datasetClient.truncate("testDataset");
    datasetClient.delete("testDataset");
    datasetClient.waitForDeleted("testDataset", 10, TimeUnit.SECONDS);
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

  @Test
  public void testSystemTypes() throws Exception {
    // Tests that a dataset can be created in a namespace, even if the type does not exist in that namespace.
    // The dataset type is being resolved from the system namespace.
    String datasetName = "tableTypeDataset";
    Assert.assertFalse(typeClient.exists(Table.class.getName()));
    Assert.assertFalse(datasetClient.exists(datasetName));
    datasetClient.create(datasetName, Table.class.getName());
    Assert.assertTrue(datasetClient.exists(datasetName));
  }
}
