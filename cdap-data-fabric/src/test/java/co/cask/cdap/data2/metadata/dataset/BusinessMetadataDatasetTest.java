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
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
    dataset = getDataset(datasetInstance);
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
    List<BusinessMetadataRecord> results =
      dataset.findBusinessMetadataOnValue("value1", MetadataSearchTargetType.PROGRAM);

    // Assert check
    Assert.assertEquals(1, results.size());

    // Case insensitive
    results = dataset.findBusinessMetadataOnValue("ValUe1", MetadataSearchTargetType.PROGRAM);

    BusinessMetadataRecord result = results.get(0);
    Assert.assertEquals(record, result);

    // Save it
    dataset.setProperty(flow1, "key3", "value1");

    // Search for it based on value
    List<BusinessMetadataRecord> results2 =
      dataset.findBusinessMetadataOnValue("value1", MetadataSearchTargetType.PROGRAM);

    // Assert check
    Assert.assertEquals(2, results2.size());

    for (BusinessMetadataRecord result2 : results2) {
      Assert.assertEquals("value1", result2.getValue());
    }

    // Save it
    dataset.setProperty(stream1, "key21", "value21");

    // Search for it based on value asterix
    List<BusinessMetadataRecord> results3 = dataset.findBusinessMetadataOnValue("value2*",
                                                                                MetadataSearchTargetType.ALL);

    // Assert check
    Assert.assertEquals(2, results3.size());
    for (BusinessMetadataRecord result3 : results3) {
      Assert.assertTrue(result3.getValue().startsWith("value2"));
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

  @Test
  public void testHistory() throws Exception {
    BusinessMetadataDataset dataset =
      getDataset(Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "testHistory"));

    doTestHistory(dataset, flow1, "f_");
    doTestHistory(dataset, app1, "a_");
    doTestHistory(dataset, dataset1, "d_");
    doTestHistory(dataset, stream1, "s_");
  }

  private void doTestHistory(BusinessMetadataDataset dataset, Id.NamespacedId targetId, String prefix)
    throws Exception {
    // Metadata change history keyed by time in millis the change was made
    Map<Long, MetadataRecord> expected = new HashMap<>();

    // No history for targetId at the beginning
    MetadataRecord completeRecord = new MetadataRecord(targetId);
    expected.put(System.currentTimeMillis(), completeRecord);
    // Get history for targetId, should be empty
    Assert.assertEquals(completeRecord,
                        dataset.getSnapshotBeforeTime(targetId, System.currentTimeMillis()));
    // Also, the metadata itself should be equal to the last recorded snapshot
    Assert.assertEquals(dataset.getSnapshotBeforeTime(targetId, System.currentTimeMillis()),
                        new MetadataRecord(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    // Since the key to expected map is time in millis, sleep for a millisecond to make sure the key is distinct
    TimeUnit.MILLISECONDS.sleep(1);

    // Add first record
    completeRecord = new MetadataRecord(targetId, toProps(prefix, "k1", "v1"), toTags(prefix, "t1", "t2"));
    addMetadataRecord(dataset, completeRecord);
    long time = System.currentTimeMillis();
    expected.put(time, completeRecord);
    // Since this is the first record, history should be the same as what was added.
    Assert.assertEquals(completeRecord, dataset.getSnapshotBeforeTime(targetId, time));
    // Also, the metadata itself should be equal to the last recorded snapshot
    Assert.assertEquals(dataset.getSnapshotBeforeTime(targetId, System.currentTimeMillis()),
                        new MetadataRecord(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    TimeUnit.MILLISECONDS.sleep(1);

    // Add a new property and a tag
    dataset.setProperty(targetId, prefix + "k2", "v2");
    dataset.addTags(targetId, prefix + "t3");
    // Save the complete metadata record at this point
    completeRecord = new MetadataRecord(targetId, toProps(prefix, "k1", "v1", "k2", "v2"),
                                        toTags(prefix, "t1", "t2", "t3"));
    time = System.currentTimeMillis();
    expected.put(time, completeRecord);
    // Assert the history record with the change
    Assert.assertEquals(completeRecord, dataset.getSnapshotBeforeTime(targetId, time));
    // Also, the metadata itself should be equal to the last recorded snapshot
    Assert.assertEquals(dataset.getSnapshotBeforeTime(targetId, System.currentTimeMillis()),
                        new MetadataRecord(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    TimeUnit.MILLISECONDS.sleep(1);

    // Add another property and a tag
    dataset.setProperty(targetId, prefix + "k3", "v3");
    dataset.addTags(targetId, prefix + "t4");
    // Save the complete metadata record at this point
    completeRecord = new MetadataRecord(targetId, toProps(prefix, "k1", "v1", "k2", "v2", "k3", "v3"),
                                        toTags(prefix, "t1", "t2", "t3", "t4"));
    time = System.currentTimeMillis();
    expected.put(time, completeRecord);
    // Assert the history record with the change
    Assert.assertEquals(completeRecord, dataset.getSnapshotBeforeTime(targetId, time));
    // Also, the metadata itself should be equal to the last recorded snapshot
    Assert.assertEquals(dataset.getSnapshotBeforeTime(targetId, System.currentTimeMillis()),
                        new MetadataRecord(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    TimeUnit.MILLISECONDS.sleep(1);

    // Add the same property and tag as second time
    dataset.setProperty(targetId, prefix + "k2", "v2");
    dataset.addTags(targetId, prefix + "t3");
    // Save the complete metadata record at this point
    completeRecord = new MetadataRecord(targetId, toProps(prefix, "k1", "v1", "k2", "v2", "k3", "v3"),
                                        toTags(prefix, "t1", "t2", "t3", "t4"));
    time = System.currentTimeMillis();
    expected.put(time, completeRecord);
    // Assert the history record with the change
    Assert.assertEquals(completeRecord, dataset.getSnapshotBeforeTime(targetId, time));
    // Also, the metadata itself should be equal to the last recorded snapshot
    Assert.assertEquals(dataset.getSnapshotBeforeTime(targetId, System.currentTimeMillis()),
                        new MetadataRecord(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    TimeUnit.MILLISECONDS.sleep(1);

    // Remove a property and two tags
    dataset.removeProperties(targetId, prefix + "k2");
    dataset.removeTags(targetId, prefix + "t4");
    dataset.removeTags(targetId, prefix + "t2");
    // Save the complete metadata record at this point
    completeRecord = new MetadataRecord(targetId, toProps(prefix, "k1", "v1", "k3", "v3"),
                                        toTags(prefix, "t1", "t3"));
    time = System.currentTimeMillis();
    expected.put(time, completeRecord);
    // Assert the history record with the change
    Assert.assertEquals(completeRecord, dataset.getSnapshotBeforeTime(targetId, time));
    // Also, the metadata itself should be equal to the last recorded snapshot
    Assert.assertEquals(dataset.getSnapshotBeforeTime(targetId, System.currentTimeMillis()),
                        new MetadataRecord(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    TimeUnit.MILLISECONDS.sleep(1);

    // Remove all properties and all tags
    dataset.removeProperties(targetId);
    dataset.removeTags(targetId);
    // Save the complete metadata record at this point
    completeRecord = new MetadataRecord(targetId);
    time = System.currentTimeMillis();
    expected.put(time, completeRecord);
    // Assert the history record with the change
    Assert.assertEquals(completeRecord, dataset.getSnapshotBeforeTime(targetId, time));
    // Also, the metadata itself should be equal to the last recorded snapshot
    Assert.assertEquals(dataset.getSnapshotBeforeTime(targetId, System.currentTimeMillis()),
                        new MetadataRecord(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    TimeUnit.MILLISECONDS.sleep(1);

    // Add one more property and a tag
    dataset.setProperty(targetId, prefix + "k2", "v2");
    dataset.addTags(targetId, prefix + "t2");
    // Save the complete metadata record at this point
    completeRecord = new MetadataRecord(targetId, toProps(prefix, "k2", "v2"),
                                        toTags(prefix, "t2"));
    time = System.currentTimeMillis();
    expected.put(time, completeRecord);
    // Assert the history record with the change
    Assert.assertEquals(completeRecord, dataset.getSnapshotBeforeTime(targetId, time));
    // Also, the metadata itself should be equal to the last recorded snapshot
    Assert.assertEquals(dataset.getSnapshotBeforeTime(targetId, System.currentTimeMillis()),
                        new MetadataRecord(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    TimeUnit.MILLISECONDS.sleep(1);

    // Now assert all history
    for (Map.Entry<Long, MetadataRecord> entry : expected.entrySet()) {
      Assert.assertEquals(entry.getValue(), dataset.getSnapshotBeforeTime(targetId, entry.getKey()));
    }

    // Asserting for current time should give the latest record
    Assert.assertEquals(completeRecord, dataset.getSnapshotBeforeTime(targetId, System.currentTimeMillis()));
    // Also, the metadata itself should be equal to the last recorded snapshot
    Assert.assertEquals(dataset.getSnapshotBeforeTime(targetId, System.currentTimeMillis()),
                        new MetadataRecord(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
  }

  private void addMetadataRecord(BusinessMetadataDataset dataset, MetadataRecord record) {
    for (Map.Entry<String, String> entry : record.getProperties().entrySet()) {
      dataset.setProperty(record.getTargetId(), entry.getKey(), entry.getValue());
    }
    //noinspection ToArrayCallWithZeroLengthArrayArgument
    dataset.addTags(record.getTargetId(), record.getTags().toArray(new String[0]));
  }

  private Map<String, String> toProps(String prefix, String k1, String v1) {
    return ImmutableMap.of(prefix + k1, v1);
  }

  private Map<String, String> toProps(String prefix, String k1, String v1, String k2, String v2) {
    return ImmutableMap.of(prefix + k1, v1, prefix + k2, v2);
  }

  private Map<String, String> toProps(String prefix, String k1, String v1, String k2, String v2, String k3, String v3) {
    return ImmutableMap.of(prefix + k1, v1, prefix + k2, v2, prefix + k3, v3);
  }

  private Set<String> toTags(String prefix, String... tags) {
    ImmutableSet.Builder<String> builder = new ImmutableSet.Builder<>();
    for (String tag : tags) {
      builder.add(prefix + tag);
    }
    return builder.build();
  }

  private static BusinessMetadataDataset getDataset(Id.DatasetInstance instance) throws Exception {
    return DatasetsUtil.getOrCreateDataset(dsFrameworkUtil.getFramework(), instance,
                                           BusinessMetadataDataset.class.getName(),
                                           DatasetProperties.EMPTY, null, null);
  }
}
