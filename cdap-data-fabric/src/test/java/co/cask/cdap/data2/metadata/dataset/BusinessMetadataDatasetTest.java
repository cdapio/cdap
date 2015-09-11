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
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.MetadataSearchTargetType;

import co.cask.cdap.proto.ProgramType;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test class for {@link BusinessMetadataDataset} class.
 */
public class BusinessMetadataDatasetTest {

  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  private static final Id.DatasetInstance datasetInstance =
    Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "meta");

  private BusinessMetadataDataset dataset;

  private final Id.Application app1 = Id.Application.from("ns1", "app1");
  // Have to use Id.Program for comparison here because the BusinessMetadataDataset APIs return Id.Program.
  private final Id.Program flow1 = Id.Program.from("ns1", "app1", ProgramType.FLOW, "flow1");
  private final Id.DatasetInstance dataset1 = Id.DatasetInstance.from("ns1", "ds1");
  private final Id.Stream stream1 = Id.Stream.from("ns1", "s1");

  @Before
  public void before() throws Exception {
    dataset = getDataset();
  }

  @After
  public void after() throws Exception {
    dataset = null;
  }

  @Test
  public void testProperties() throws Exception {
    Assert.assertEquals(0, dataset.getProperties(app1).size());
    Assert.assertEquals(0, dataset.getProperties(flow1).size());
    Assert.assertEquals(0, dataset.getProperties(dataset1).size());
    Assert.assertEquals(0, dataset.getProperties(stream1).size());
    // Set some properties
    dataset.setProperty(app1, "akey1", "avalue1");
    dataset.setProperty(flow1, "fkey1", "fvalue1");
    dataset.setProperty(flow1, "fK", "fV");
    dataset.setProperty(dataset1, "dkey1", "dvalue1");
    dataset.setProperty(stream1, "skey1", "svalue1");
    dataset.setProperty(stream1, "skey2", "svalue2");
    // verify
    Map<String, String> properties = dataset.getProperties(app1);
    Assert.assertEquals(ImmutableMap.of("akey1", "avalue1"), properties);
    dataset.removeProperties(app1, "akey1");
    Assert.assertNull(dataset.getProperty(app1, "akey1"));
    BusinessMetadataRecord result = dataset.getProperty(flow1, "fkey1");
    BusinessMetadataRecord expected = new BusinessMetadataRecord(flow1, "fkey1", "fvalue1");
    Assert.assertEquals(expected, result);
    Assert.assertEquals(ImmutableMap.of("fkey1", "fvalue1", "fK", "fV"), dataset.getProperties(flow1));
    dataset.removeProperties(flow1, "fkey1");
    properties = dataset.getProperties(flow1);
    Assert.assertEquals(1, properties.size());
    Assert.assertEquals("fV", properties.get("fK"));
    dataset.removeProperties(flow1);
    Assert.assertEquals(0, dataset.getProperties(flow1).size());
    expected = new BusinessMetadataRecord(dataset1, "dkey1", "dvalue1");
    Assert.assertEquals(expected, dataset.getProperty(dataset1, "dkey1"));
    Assert.assertEquals(ImmutableMap.of("skey1", "svalue1", "skey2", "svalue2"), dataset.getProperties(stream1));
    // reset a property
    dataset.setProperty(stream1, "skey1", "sv1");
    Assert.assertEquals(ImmutableMap.of("skey1", "sv1", "skey2", "svalue2"), dataset.getProperties(stream1));
    // cleanup
    dataset.removeProperties(app1);
    dataset.removeProperties(flow1);
    dataset.removeProperties(dataset1);
    dataset.removeProperties(stream1);
    Assert.assertEquals(0, dataset.getProperties(app1).size());
    Assert.assertEquals(0, dataset.getProperties(flow1).size());
    Assert.assertEquals(0, dataset.getProperties(dataset1).size());
    Assert.assertEquals(0, dataset.getProperties(stream1).size());
  }

  @Test
  public void testTags() {
    Assert.assertEquals(0, dataset.getTags(app1).size());
    Assert.assertEquals(0, dataset.getTags(flow1).size());
    Assert.assertEquals(0, dataset.getTags(dataset1).size());
    Assert.assertEquals(0, dataset.getTags(stream1).size());
    dataset.addTags(app1, "tag1", "tag2", "tag3");
    dataset.addTags(flow1, "tag1");
    dataset.addTags(dataset1, "tag3", "tag2");
    dataset.addTags(stream1, "tag2");
    Set<String> tags = dataset.getTags(app1);
    Assert.assertEquals(3, tags.size());
    Assert.assertTrue(tags.contains("tag1"));
    Assert.assertTrue(tags.contains("tag2"));
    Assert.assertTrue(tags.contains("tag3"));
    // add the same tag again
    dataset.addTags(app1, "tag1");
    Assert.assertEquals(3, dataset.getTags(app1).size());
    tags = dataset.getTags(flow1);
    Assert.assertEquals(1, tags.size());
    Assert.assertTrue(tags.contains("tag1"));
    tags = dataset.getTags(dataset1);
    Assert.assertEquals(2, tags.size());
    Assert.assertTrue(tags.contains("tag3"));
    Assert.assertTrue(tags.contains("tag2"));
    tags = dataset.getTags(stream1);
    Assert.assertEquals(1, tags.size());
    Assert.assertTrue(tags.contains("tag2"));
    dataset.removeTags(app1, "tag1", "tag2");
    tags = dataset.getTags(app1);
    Assert.assertEquals(1, tags.size());
    Assert.assertTrue(tags.contains("tag3"));
    dataset.removeTags(dataset1, "tag3");
    tags = dataset.getTags(dataset1);
    Assert.assertEquals(1, tags.size());
    Assert.assertTrue(tags.contains("tag2"));
    // cleanup
    dataset.removeTags(app1);
    dataset.removeTags(flow1);
    dataset.removeTags(dataset1);
    dataset.removeTags(stream1);
    Assert.assertEquals(0, dataset.getTags(app1).size());
    Assert.assertEquals(0, dataset.getTags(flow1).size());
    Assert.assertEquals(0, dataset.getTags(dataset1).size());
    Assert.assertEquals(0, dataset.getTags(stream1).size());
  }

  @Test
  public void testSearchOnValue() throws Exception {
    // Create record
    BusinessMetadataRecord record = new BusinessMetadataRecord(flow1, "key1", "value1");
    // Save it
    dataset.setProperty(flow1, "key1", "value1");

    // Save it
    dataset.setProperty(flow1, "key2", "value2");

    // Search for it based on value
    List<BusinessMetadataRecord> results = dataset.findBusinessMetadataOnValue("value1",
                                                                               MetadataSearchTargetType.PROGRAM);

    // Assert check
    Assert.assertEquals(1, results.size());

    BusinessMetadataRecord result = results.get(0);
    Assert.assertEquals(record, result);

    // Save it
    dataset.setProperty(flow1, "key3", "value1");

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
    // Create record
    BusinessMetadataRecord record = new BusinessMetadataRecord(flow1, "key1", "value1");
    // Save it
    dataset.setProperty(flow1, "key1", "value1");

    // Save it
    dataset.setProperty(flow1, "key2", "value2");

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
