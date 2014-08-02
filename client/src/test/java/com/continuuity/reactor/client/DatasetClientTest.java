/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.reactor.client;

import com.continuuity.client.DatasetClient;
import com.continuuity.client.DatasetModuleClient;
import com.continuuity.client.DatasetTypeClient;
import com.continuuity.client.config.ReactorClientConfig;
import com.continuuity.proto.DatasetModuleMeta;
import com.continuuity.proto.DatasetTypeMeta;
import com.continuuity.reactor.client.app.FakeDataset;
import com.continuuity.reactor.client.app.FakeDatasetModule;
import com.continuuity.reactor.client.common.ClientTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Test for {@link DatasetClient}, {@link DatasetModuleClient}, and {@link DatasetTypeClient}.
 */
public class DatasetClientTest extends ClientTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetClientTest.class);

  private DatasetClient datasetClient;
  private DatasetModuleClient moduleClient;
  private DatasetTypeClient typeClient;

  @Before
  public void setUp() throws Throwable {
    super.setUp();

    ReactorClientConfig config = new ReactorClientConfig("localhost");
    datasetClient = new DatasetClient(config);
    moduleClient = new DatasetModuleClient(config);
    typeClient = new DatasetTypeClient(config);
  }

  @Test
  public void testAll() throws Exception {
    int numBaseModules = moduleClient.list().size();
    int numBaseTypes = typeClient.list().size();
    Assert.assertEquals(0, datasetClient.list().size());

    LOG.info("Adding Dataset module");
    File moduleJarFile = createAppJarFile(FakeDatasetModule.class);
    moduleClient.add(FakeDatasetModule.NAME, FakeDatasetModule.class.getName(), moduleJarFile);
    Assert.assertEquals(numBaseModules + 1, moduleClient.list().size());
    Assert.assertEquals(numBaseTypes + 2, typeClient.list().size());

    LOG.info("Checking that the new Dataset module exists");
    DatasetModuleMeta datasetModuleMeta = moduleClient.get(FakeDatasetModule.NAME);
    Assert.assertNotNull(datasetModuleMeta);
    Assert.assertEquals(FakeDatasetModule.NAME, datasetModuleMeta.getName());

    LOG.info("Checking that the new Dataset type exists");
    DatasetTypeMeta datasetTypeMeta = typeClient.get(FakeDataset.TYPE_NAME);
    Assert.assertNotNull(datasetTypeMeta);
    Assert.assertEquals(FakeDataset.TYPE_NAME, datasetTypeMeta.getName());

    datasetTypeMeta = typeClient.get(FakeDataset.class.getName());
    Assert.assertNotNull(datasetTypeMeta);
    Assert.assertEquals(FakeDataset.class.getName(), datasetTypeMeta.getName());

    LOG.info("Creating, truncating, and deleting dataset of new Dataset type");
    Assert.assertEquals(0, datasetClient.list().size());
    datasetClient.create("testDataset", FakeDataset.TYPE_NAME);
    Assert.assertEquals(1, datasetClient.list().size());
    datasetClient.truncate("testDataset");
    datasetClient.delete("testDataset");
    Assert.assertEquals(0, datasetClient.list().size());

    LOG.info("Creating and deleting multiple Datasets");
    datasetClient.create("testDataset1", FakeDataset.TYPE_NAME);
    datasetClient.create("testDataset2", FakeDataset.TYPE_NAME);
    datasetClient.create("testDataset3", FakeDataset.TYPE_NAME);
    Assert.assertEquals(3, datasetClient.list().size());
    datasetClient.deleteAll();
    Assert.assertEquals(0, datasetClient.list().size());

    LOG.info("Deleting Dataset module");
    moduleClient.delete(FakeDatasetModule.NAME);
    Assert.assertEquals(numBaseModules, moduleClient.list().size());
    Assert.assertEquals(numBaseTypes, typeClient.list().size());

    LOG.info("Adding Dataset module and then deleting all Dataset modules");
    moduleClient.add("testModule1", FakeDatasetModule.class.getName(), moduleJarFile);
    Assert.assertEquals(numBaseModules + 1, moduleClient.list().size());
    Assert.assertEquals(numBaseTypes + 2, typeClient.list().size());
    moduleClient.deleteAll();
    Assert.assertEquals(numBaseModules, moduleClient.list().size());
    Assert.assertEquals(numBaseTypes, typeClient.list().size());
  }
}
