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
import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.DatasetModuleNotFoundException;
import co.cask.cdap.common.DatasetTypeNotFoundException;
import co.cask.cdap.proto.DatasetMeta;
import co.cask.cdap.proto.DatasetModuleMeta;
import co.cask.cdap.proto.DatasetTypeMeta;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.test.XSlowTests;
import com.google.common.collect.ImmutableMap;
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
  private static final Id.Namespace TEST_NAMESPACE = Id.Namespace.from("testNamespace");
  private static final Id.Namespace OTHER_NAMESPACE = Id.Namespace.from("otherNamespace");

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
      namespaceClient.create(new NamespaceMeta.Builder().setName(TEST_NAMESPACE).build());
    } catch (AlreadyExistsException e) {
      // expected
    }
    try {
      namespaceClient.create(new NamespaceMeta.Builder().setName(OTHER_NAMESPACE).build());
    } catch (AlreadyExistsException e) {
      // expected
    }
  }

  @After
  public void tearDown() throws Exception {
    NamespaceClient namespaceClient = new NamespaceClient(clientConfig);
    namespaceClient.delete(TEST_NAMESPACE);
    namespaceClient.delete(OTHER_NAMESPACE);
  }

  @Test
  public void testAll() throws Exception {
    Id.DatasetModule module = Id.DatasetModule.from(TEST_NAMESPACE, StandaloneDatasetModule.NAME);
    Id.DatasetType type = Id.DatasetType.from(TEST_NAMESPACE, StandaloneDataset.class.getName());
    Id.DatasetModule moduleInOtherNamespace = Id.DatasetModule.from(OTHER_NAMESPACE, StandaloneDatasetModule.NAME);
    Id.DatasetType typeInOtherNamespace = Id.DatasetType.from(OTHER_NAMESPACE, StandaloneDataset.class.getName());

    int numBaseModules = moduleClient.list(TEST_NAMESPACE).size();
    int numBaseTypes = typeClient.list(TEST_NAMESPACE).size();

    LOG.info("Adding Dataset module");
    File moduleJarFile = createAppJarFile(StandaloneDatasetModule.class);
    moduleClient.add(Id.DatasetModule.from(TEST_NAMESPACE, StandaloneDatasetModule.NAME),
                     StandaloneDatasetModule.class.getName(), moduleJarFile);
    moduleClient.waitForExists(module, 30, TimeUnit.SECONDS);
    Assert.assertEquals(numBaseModules + 1, moduleClient.list(TEST_NAMESPACE).size());
    Assert.assertEquals(numBaseTypes + 2, typeClient.list(TEST_NAMESPACE).size());

    LOG.info("Checking that the new Dataset module exists");
    DatasetModuleMeta datasetModuleMeta = moduleClient.get(module);
    Assert.assertNotNull(datasetModuleMeta);
    Assert.assertEquals(StandaloneDatasetModule.NAME, datasetModuleMeta.getName());

    LOG.info("Checking that the new Dataset module does not exist in a different namespace");
    try {
      moduleClient.get(moduleInOtherNamespace);
      Assert.fail("datasetModule found in namespace other than one in which it was expected");
    } catch (DatasetModuleNotFoundException expected) {
      // expected
    }

    LOG.info("Checking that the new Dataset type exists");
    typeClient.waitForExists(type, 5, TimeUnit.SECONDS);
    DatasetTypeMeta datasetTypeMeta = typeClient.get(type);
    Assert.assertNotNull(datasetTypeMeta);
    Assert.assertEquals(type.getId(), datasetTypeMeta.getName());

    datasetTypeMeta = typeClient.get(type);
    Assert.assertNotNull(datasetTypeMeta);
    Assert.assertEquals(StandaloneDataset.class.getName(), datasetTypeMeta.getName());

    LOG.info("Checking that the new Dataset type does not exist in a different namespace");
    try {
      typeClient.get(typeInOtherNamespace);
      Assert.fail("datasetType found in namespace other than one in which it was expected");
    } catch (DatasetTypeNotFoundException expected) {
      // expected
    }

    LOG.info("Creating, truncating, and deleting dataset of new Dataset type");
    // Before creating dataset, there are some system datasets already exist
    int numBaseDataset = datasetClient.list(TEST_NAMESPACE).size();

    Id.DatasetInstance instance = Id.DatasetInstance.from(TEST_NAMESPACE, "testDataset");

    datasetClient.create(instance, StandaloneDataset.TYPE_NAME);
    Assert.assertEquals(numBaseDataset + 1, datasetClient.list(TEST_NAMESPACE).size());
    datasetClient.truncate(instance);

    DatasetMeta metaBefore = datasetClient.get(instance);
    Assert.assertEquals(0, metaBefore.getSpec().getProperties().size());

    datasetClient.update(instance, ImmutableMap.of("sdf", "foo", "abc", "123"));
    DatasetMeta metaAfter = datasetClient.get(instance);
    Assert.assertEquals(2, metaAfter.getSpec().getProperties().size());
    Assert.assertTrue(metaAfter.getSpec().getProperties().containsKey("sdf"));
    Assert.assertTrue(metaAfter.getSpec().getProperties().containsKey("abc"));
    Assert.assertEquals("foo", metaAfter.getSpec().getProperties().get("sdf"));
    Assert.assertEquals("123", metaAfter.getSpec().getProperties().get("abc"));

    datasetClient.updateExisting(instance, ImmutableMap.of("sdf", "fzz"));
    metaAfter = datasetClient.get(instance);
    Assert.assertEquals(2, metaAfter.getSpec().getProperties().size());
    Assert.assertTrue(metaAfter.getSpec().getProperties().containsKey("sdf"));
    Assert.assertTrue(metaAfter.getSpec().getProperties().containsKey("abc"));
    Assert.assertEquals("fzz", metaAfter.getSpec().getProperties().get("sdf"));
    Assert.assertEquals("123", metaAfter.getSpec().getProperties().get("abc"));

    datasetClient.delete(instance);
    datasetClient.waitForDeleted(instance, 10, TimeUnit.SECONDS);
    Assert.assertEquals(numBaseDataset, datasetClient.list(TEST_NAMESPACE).size());

    LOG.info("Creating and deleting multiple Datasets");
    for (int i = 1; i <= 3; i++) {
      datasetClient.create(Id.DatasetInstance.from(TEST_NAMESPACE, "testDataset" + i), StandaloneDataset.TYPE_NAME);
    }
    Assert.assertEquals(numBaseDataset + 3, datasetClient.list(TEST_NAMESPACE).size());
    for (int i = 1; i <= 3; i++) {
      datasetClient.delete(Id.DatasetInstance.from(TEST_NAMESPACE, "testDataset" + i));
    }
    Assert.assertEquals(numBaseDataset, datasetClient.list(TEST_NAMESPACE).size());

    LOG.info("Deleting Dataset module");
    moduleClient.delete(module);
    Assert.assertEquals(numBaseModules, moduleClient.list(TEST_NAMESPACE).size());
    Assert.assertEquals(numBaseTypes, typeClient.list(TEST_NAMESPACE).size());

    LOG.info("Adding Dataset module and then deleting all Dataset modules");
    moduleClient.add(Id.DatasetModule.from(TEST_NAMESPACE, "testModule1"),
                     StandaloneDatasetModule.class.getName(), moduleJarFile);
    Assert.assertEquals(numBaseModules + 1, moduleClient.list(TEST_NAMESPACE).size());
    Assert.assertEquals(numBaseTypes + 2, typeClient.list(TEST_NAMESPACE).size());

    moduleClient.deleteAll(TEST_NAMESPACE);
    Assert.assertEquals(numBaseModules, moduleClient.list(TEST_NAMESPACE).size());
    Assert.assertEquals(numBaseTypes, typeClient.list(TEST_NAMESPACE).size());
  }

  @Test
  public void testSystemTypes() throws Exception {
    // Tests that a dataset can be created in a namespace, even if the type does not exist in that namespace.
    // The dataset type is being resolved from the system namespace.
    Id.DatasetType type = Id.DatasetType.from(TEST_NAMESPACE, Table.class.getName());
    Id.DatasetInstance instance = Id.DatasetInstance.from(TEST_NAMESPACE, "tableTypeDataset");

    Assert.assertFalse(typeClient.exists(type));
    Assert.assertFalse(datasetClient.exists(instance));
    datasetClient.create(instance, Table.class.getName());
    Assert.assertTrue(datasetClient.exists(instance));
  }
}
