/*
 * Copyright 2015 Cask Data, Inc.
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
package co.cask.cdap.data2.metadata.dataset;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.proto.BusinessMetadataRecord;
import co.cask.cdap.proto.Id;

import co.cask.cdap.proto.ProgramType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test class for {@link BusinessMetadataDataset} class.
 */
public class BusinessMetadataDatasetTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  private static final Id.DatasetInstance datasetInstance =
    Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "meta");

  BusinessMetadataDataset dataset;

  @Before
  public void before() throws Exception {
    // TODO Add code
    dataset = getDataset();
  }

  @After
  public void after() throws Exception {
    // TODO Add code
  }

  @Test
  public void testAddOneMetadata() throws Exception {
    // TODO ADD CODE

    Id.Program flow21 = Id.Program.from("ns1", "app2", ProgramType.FLOW, "flow21");

    // Create record
    final BusinessMetadataRecord record = new BusinessMetadataRecord(Id.Program.class.getSimpleName(),
                                                                     flow21, "key1", "value1");

    // Save it
    dataset.createBusinessMetadata(record);

    // Get that record
    final BusinessMetadataRecord result = dataset.getBusinessMetadata(flow21, "key1");

    // Assert check
    Assert.assertEquals(record, result);
  }

  private static BusinessMetadataDataset getDataset() throws Exception {
    return DatasetsUtil.getOrCreateDataset(dsFrameworkUtil.getFramework(), datasetInstance,
                                           BusinessMetadataDataset.class.getSimpleName(),
                                           DatasetProperties.EMPTY, null, null);
  }
}
