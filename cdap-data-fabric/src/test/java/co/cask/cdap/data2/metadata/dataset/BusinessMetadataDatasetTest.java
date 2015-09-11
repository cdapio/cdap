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
import co.cask.cdap.data2.metadata.service.BusinessMetadataStore;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.MetadataSearchTargetType;
import co.cask.cdap.proto.ProgramType;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Test class for {@link BusinessMetadataDataset} class.
 */
public class BusinessMetadataDatasetTest {

  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  private static final Id.DatasetInstance datasetInstance =
    Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "meta");

  BusinessMetadataDataset dataset;

  @Before
  public void before() throws Exception {
    dataset = getDataset();
  }

  @After
  public void after() throws Exception {
    dataset = null;
  }

  @Test
  public void testAddOneMetadata() throws Exception {
    Id.Program flow21 = Id.Program.from("ns1", "app2", ProgramType.FLOW, "flow21");
    // Create record
    BusinessMetadataRecord record = new BusinessMetadataRecord(
      flow21, "key1", "value1");
    // Save it
    dataset.createBusinessMetadata(record);
    // Get that record
    BusinessMetadataRecord result = dataset.getBusinessMetadata(flow21, "key1");
    // Assert check
    Assert.assertEquals(record, result);
    dataset.removeMetadata(flow21, "key");
    Map<String, String> businessMetadata = dataset.getBusinessMetadata(flow21);
    Assert.assertEquals(1, businessMetadata.size());
    Assert.assertEquals("value1", businessMetadata.get("key1"));
    dataset.removeMetadata(flow21);
    businessMetadata = dataset.getBusinessMetadata(flow21);
    Assert.assertEquals(0, businessMetadata.size());
  }

  @Test
  public void testSearchOnValue() throws Exception {
    Id.Program flow21 = Id.Program.from("ns1", "app2", ProgramType.FLOW, "flow21");
    // Create record
    BusinessMetadataRecord record = new BusinessMetadataRecord(flow21, "key1", "value1");
    // Save it
    dataset.createBusinessMetadata(record);

    // Create record
    BusinessMetadataRecord record2 = new BusinessMetadataRecord(flow21, "key2", "value2");
    // Save it
    dataset.createBusinessMetadata(record2);

    // Search for it based on value
    List<BusinessMetadataRecord> results = dataset.findBusinessMetadataOnValue("value1",
                                                                               MetadataSearchTargetType.PROGRAM);

    // Assert check
    Assert.assertEquals(1, results.size());

    BusinessMetadataRecord result = results.get(0);
    Assert.assertEquals(record, result);

    // Create record
    BusinessMetadataRecord record3 = new BusinessMetadataRecord(flow21, "key3", "value1");
    // Save it
    dataset.createBusinessMetadata(record3);

    // Search for it based on value
    List<BusinessMetadataRecord> results2 = dataset.findBusinessMetadataOnValue("value1",
                                                                                MetadataSearchTargetType.PROGRAM);

    // Assert check
    Assert.assertEquals(2, results2.size());

    for (BusinessMetadataRecord result2 : results2) {
      Assert.assertEquals("value1", result2.getValue());
    }
  }

  @Test
  public void testSearchOnKeyValue() throws Exception {
    Id.Program flow21 = Id.Program.from("ns1", "app2", ProgramType.FLOW, "flow21");
    // Create record
    BusinessMetadataRecord record = new BusinessMetadataRecord(flow21, "key1", "value1");
    // Save it
    dataset.createBusinessMetadata(record);

    // Create record
    BusinessMetadataRecord record2 = new BusinessMetadataRecord(flow21, "key2", "value2");
    // Save it
    dataset.createBusinessMetadata(record2);

    // Search for it based on value
    List<BusinessMetadataRecord> results =
      dataset.findBusinessMetadataOnKeyValue("key1" + BusinessMetadataDataset.KEYVALUE_SEPARATOR + "value1",
                                             MetadataSearchTargetType.PROGRAM);

    // Assert check
    Assert.assertEquals(1, results.size());

    BusinessMetadataRecord result = results.get(0);
    Assert.assertEquals(record, result);

  }

  private static BusinessMetadataDataset getDataset() throws Exception {
    return DatasetsUtil.getOrCreateDataset(dsFrameworkUtil.getFramework(), datasetInstance,
                                           BusinessMetadataDataset.class.getName(),
                                           DatasetProperties.EMPTY, null, null);
  }
}
