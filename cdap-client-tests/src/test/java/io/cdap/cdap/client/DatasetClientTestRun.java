/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package io.cdap.cdap.client;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.client.common.ClientTestBase;
import io.cdap.cdap.common.AlreadyExistsException;
import io.cdap.cdap.common.DatasetModuleNotFoundException;
import io.cdap.cdap.common.DatasetTypeNotFoundException;
import io.cdap.cdap.proto.DatasetInstanceConfiguration;
import io.cdap.cdap.proto.DatasetMeta;
import io.cdap.cdap.proto.DatasetModuleMeta;
import io.cdap.cdap.proto.DatasetSpecificationSummary;
import io.cdap.cdap.proto.DatasetTypeMeta;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.DatasetTypeId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.XSlowTests;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for {@link DatasetClient}, {@link DatasetModuleClient}, and {@link DatasetTypeClient}.
 */
@Category(XSlowTests.class)
public class DatasetClientTestRun extends ClientTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetClientTestRun.class);
  private static final NamespaceId TEST_NAMESPACE = new NamespaceId("testNamespace");
  private static final NamespaceId OTHER_NAMESPACE = new NamespaceId("otherNamespace");
  // a dataset type that is defined by the system
  private static final String DATASET_TYPE = "keyValueTable";

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
    DatasetModuleId module = TEST_NAMESPACE.datasetModule("testModule");
    DatasetTypeId type = TEST_NAMESPACE.datasetType(DATASET_TYPE);
    DatasetTypeId systemType = NamespaceId.SYSTEM.datasetType(DATASET_TYPE);

    LOG.info("Checking that no Dataset module exists in the namespace");
    Assert.assertTrue(moduleClient.list(TEST_NAMESPACE).isEmpty());
    Assert.assertTrue(typeClient.list(TEST_NAMESPACE).isEmpty());
    Assert.assertFalse(moduleClient.exists(module));
    try {
      moduleClient.get(module);
      Assert.fail("datasetModule found in namespace that has no modules");
    } catch (DatasetModuleNotFoundException expected) {
      // expected
    }
    try {
      moduleClient.delete(module);
      Assert.fail("datasetModule deleted in namespace that has no modules");
    } catch (DatasetModuleNotFoundException expected) {
      // expected
    }

    LOG.info("Checking that the Dataset type exists in the system namespace");
    DatasetTypeMeta datasetTypeMeta = typeClient.get(systemType);
    Assert.assertNotNull(datasetTypeMeta);
    Assert.assertEquals(DATASET_TYPE, datasetTypeMeta.getName());
    Assert.assertFalse(datasetTypeMeta.getModules().isEmpty());

    LOG.info("Checking that the Dataset modules of the type exist in the system namespace");
    List<DatasetModuleMeta> systemModules = moduleClient.list(NamespaceId.SYSTEM);
    for (DatasetModuleMeta moduleMeta : datasetTypeMeta.getModules()) {
      DatasetModuleMeta datasetModuleMeta = moduleClient.get(NamespaceId.SYSTEM.datasetModule(moduleMeta.getName()));
      Assert.assertNotNull(datasetModuleMeta);
      Assert.assertEquals(moduleMeta.getName(), datasetModuleMeta.getName());
      Assert.assertTrue(systemModules.contains(datasetModuleMeta));
    }

    LOG.info("Checking that the Dataset type does not exist in a different namespace");
    try {
      typeClient.get(type);
      Assert.fail("datasetType found in namespace other than one in which it was expected");
    } catch (DatasetTypeNotFoundException expected) {
      // expected
    }

    LOG.info("Creating, truncating, and deleting dataset of the Dataset type");
    // Before creating dataset, there are some system datasets already exist
    int numBaseDataset = datasetClient.list(TEST_NAMESPACE).size();

    DatasetId instance = TEST_NAMESPACE.dataset("testDataset");

    String description = "test description";
    datasetClient.create(instance,
                         new DatasetInstanceConfiguration(DATASET_TYPE,
                                                          Collections.<String, String>emptyMap(), description, null));
    Assert.assertEquals(numBaseDataset + 1, datasetClient.list(TEST_NAMESPACE).size());

    // Assert dataset summary for the newly created dataset
    DatasetSpecificationSummary expectedSpec =
      new DatasetSpecificationSummary(instance.getDataset(), DATASET_TYPE, description,
                                      Collections.<String, String>emptyMap());
    Assert.assertEquals(expectedSpec, getSpecForDataset(instance, datasetClient.list(TEST_NAMESPACE)));
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
    Assert.assertEquals(numBaseDataset, datasetClient.list(TEST_NAMESPACE).size());

    LOG.info("Creating and deleting multiple Datasets");
    for (int i = 1; i <= 3; i++) {
      datasetClient.create(TEST_NAMESPACE.dataset("testDataset" + i), DATASET_TYPE);
    }
    Assert.assertEquals(numBaseDataset + 3, datasetClient.list(TEST_NAMESPACE).size());
    for (int i = 1; i <= 3; i++) {
      datasetClient.delete(TEST_NAMESPACE.dataset("testDataset" + i));
    }
    Assert.assertEquals(numBaseDataset, datasetClient.list(TEST_NAMESPACE).size());

    LOG.info("Deleting all Dataset modules in the namespace");
    moduleClient.deleteAll(TEST_NAMESPACE);
    Assert.assertTrue(moduleClient.list(TEST_NAMESPACE).isEmpty());
    Assert.assertEquals(systemModules, moduleClient.list(NamespaceId.SYSTEM));
  }

  @Test
  public void testSystemTypes() throws Exception {
    // Tests that a dataset can be created in a namespace, even if the type does not exist in that namespace.
    // The dataset type is being resolved from the system namespace.
    DatasetTypeId type = TEST_NAMESPACE.datasetType(Table.class.getName());
    DatasetId instance = TEST_NAMESPACE.dataset("tableTypeDataset");

    Assert.assertFalse(typeClient.exists(type));
    Assert.assertFalse(datasetClient.exists(instance));
    datasetClient.create(instance, Table.class.getName());
    Assert.assertTrue(datasetClient.exists(instance));
  }

  @Nullable
  private DatasetSpecificationSummary getSpecForDataset(DatasetId instance,
                                                        List<DatasetSpecificationSummary> summaries) {
    DatasetSpecificationSummary actualSummary = null;
    for (DatasetSpecificationSummary summary : summaries) {
      if (DATASET_TYPE.equals(summary.getType()) && instance.getDataset().equals(summary.getName())) {
        actualSummary = summary;
      }
    }
    return actualSummary;
  }
}
