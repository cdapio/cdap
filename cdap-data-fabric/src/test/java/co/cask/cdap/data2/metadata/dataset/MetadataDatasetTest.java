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
import co.cask.cdap.proto.metadata.MetadataSearchTargetType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
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
 * Test class for {@link MetadataDataset} class.
 */
public class MetadataDatasetTest {

  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  private static final Id.DatasetInstance datasetInstance =
    Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "meta");

  private MetadataDataset dataset;

  private final Id.Application app1 = Id.Application.from("ns1", "app1");
  // Have to use Id.Program for comparison here because the MetadataDataset APIs return Id.Program.
  private final Id.Program flow1 = Id.Program.from("ns1", "app1", ProgramType.FLOW, "flow1");
  private final Id.DatasetInstance dataset1 = Id.DatasetInstance.from("ns1", "ds1");
  private final Id.Stream stream1 = Id.Stream.from("ns1", "s1");
  private final Id.Stream.View view1 = Id.Stream.View.from(stream1, "v1");
  private final Id.Artifact artifact1 = Id.Artifact.from(Id.Namespace.from("ns1"), "a1", "1.0.0");

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
    Assert.assertEquals(0, dataset.getProperties(view1).size());
    Assert.assertEquals(0, dataset.getProperties(artifact1).size());
    // Set some properties
    dataset.setProperty(app1, "akey1", "avalue1");
    dataset.setProperty(flow1, "fkey1", "fvalue1");
    dataset.setProperty(flow1, "fK", "fV");
    dataset.setProperty(dataset1, "dkey1", "dvalue1");
    dataset.setProperty(stream1, "skey1", "svalue1");
    dataset.setProperty(stream1, "skey2", "svalue2");
    dataset.setProperty(view1, "vkey1", "vvalue1");
    dataset.setProperty(view1, "vkey2", "vvalue2");
    dataset.setProperty(artifact1, "rkey1", "rvalue1");
    dataset.setProperty(artifact1, "rkey2", "rvalue2");
    // verify
    Map<String, String> properties = dataset.getProperties(app1);
    Assert.assertEquals(ImmutableMap.of("akey1", "avalue1"), properties);
    dataset.removeProperties(app1, "akey1");
    Assert.assertNull(dataset.getProperty(app1, "akey1"));
    MetadataEntry result = dataset.getProperty(flow1, "fkey1");
    MetadataEntry expected = new MetadataEntry(flow1, "fkey1", "fvalue1");
    Assert.assertEquals(expected, result);
    Assert.assertEquals(ImmutableMap.of("fkey1", "fvalue1", "fK", "fV"), dataset.getProperties(flow1));
    dataset.removeProperties(flow1, "fkey1");
    properties = dataset.getProperties(flow1);
    Assert.assertEquals(1, properties.size());
    Assert.assertEquals("fV", properties.get("fK"));
    dataset.removeProperties(flow1);
    Assert.assertEquals(0, dataset.getProperties(flow1).size());
    expected = new MetadataEntry(dataset1, "dkey1", "dvalue1");
    Assert.assertEquals(expected, dataset.getProperty(dataset1, "dkey1"));
    Assert.assertEquals(ImmutableMap.of("skey1", "svalue1", "skey2", "svalue2"), dataset.getProperties(stream1));
    properties = dataset.getProperties(artifact1);
    Assert.assertEquals(ImmutableMap.of("rkey1", "rvalue1", "rkey2", "rvalue2"), properties);
    result = dataset.getProperty(artifact1, "rkey2");
    expected = new MetadataEntry(artifact1, "rkey2", "rvalue2");
    Assert.assertEquals(expected, result);
    properties = dataset.getProperties(view1);
    Assert.assertEquals(ImmutableMap.of("vkey1", "vvalue1", "vkey2", "vvalue2"), properties);
    result = dataset.getProperty(view1, "vkey2");
    expected = new MetadataEntry(view1, "vkey2", "vvalue2");
    Assert.assertEquals(expected, result);
    // reset a property
    dataset.setProperty(stream1, "skey1", "sv1");
    Assert.assertEquals(ImmutableMap.of("skey1", "sv1", "skey2", "svalue2"), dataset.getProperties(stream1));
    // cleanup
    dataset.removeProperties(app1);
    dataset.removeProperties(flow1);
    dataset.removeProperties(dataset1);
    dataset.removeProperties(stream1);
    dataset.removeProperties(artifact1);
    dataset.removeProperties(view1);
    Assert.assertEquals(0, dataset.getProperties(app1).size());
    Assert.assertEquals(0, dataset.getProperties(flow1).size());
    Assert.assertEquals(0, dataset.getProperties(dataset1).size());
    Assert.assertEquals(0, dataset.getProperties(stream1).size());
    Assert.assertEquals(0, dataset.getProperties(view1).size());
    Assert.assertEquals(0, dataset.getProperties(artifact1).size());
  }

  @Test
  public void testTags() {
    Assert.assertEquals(0, dataset.getTags(app1).size());
    Assert.assertEquals(0, dataset.getTags(flow1).size());
    Assert.assertEquals(0, dataset.getTags(dataset1).size());
    Assert.assertEquals(0, dataset.getTags(stream1).size());
    Assert.assertEquals(0, dataset.getTags(view1).size());
    Assert.assertEquals(0, dataset.getTags(artifact1).size());
    dataset.addTags(app1, "tag1", "tag2", "tag3");
    dataset.addTags(flow1, "tag1");
    dataset.addTags(dataset1, "tag3", "tag2");
    dataset.addTags(stream1, "tag2");
    dataset.addTags(view1, "tag4");
    dataset.addTags(artifact1, "tag3");
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
    tags = dataset.getTags(view1);
    Assert.assertEquals(1, tags.size());
    Assert.assertTrue(tags.contains("tag4"));
    dataset.removeTags(app1, "tag1", "tag2");
    tags = dataset.getTags(app1);
    Assert.assertEquals(1, tags.size());
    Assert.assertTrue(tags.contains("tag3"));
    dataset.removeTags(dataset1, "tag3");
    tags = dataset.getTags(dataset1);
    Assert.assertEquals(1, tags.size());
    Assert.assertTrue(tags.contains("tag2"));
    tags = dataset.getTags(artifact1);
    Assert.assertEquals(1, tags.size());
    Assert.assertTrue(tags.contains("tag3"));
    // cleanup
    dataset.removeTags(app1);
    dataset.removeTags(flow1);
    dataset.removeTags(dataset1);
    dataset.removeTags(stream1);
    dataset.removeTags(view1);
    dataset.removeTags(artifact1);
    Assert.assertEquals(0, dataset.getTags(app1).size());
    Assert.assertEquals(0, dataset.getTags(flow1).size());
    Assert.assertEquals(0, dataset.getTags(dataset1).size());
    Assert.assertEquals(0, dataset.getTags(stream1).size());
    Assert.assertEquals(0, dataset.getTags(view1).size());
    Assert.assertEquals(0, dataset.getTags(artifact1).size());
  }

  @Test
  public void testSearchOnTags() throws Exception {
    Assert.assertEquals(0, dataset.getTags(app1).size());
    Assert.assertEquals(0, dataset.getTags(flow1).size());
    Assert.assertEquals(0, dataset.getTags(dataset1).size());
    Assert.assertEquals(0, dataset.getTags(stream1).size());
    dataset.addTags(app1, "tag1", "tag2", "tag3");
    dataset.addTags(flow1, "tag1");
    dataset.addTags(dataset1, "tag3", "tag2", "tag12");
    dataset.addTags(stream1, "tag2, tag4");

    // Try to search on all tags
    List<MetadataEntry> results =
      dataset.searchByKeyValue("ns1", "tags:*", MetadataSearchTargetType.ALL);
    Assert.assertEquals(4, results.size());

    // Try to search for tag1*
    results = dataset.searchByKeyValue("ns1", "tags:tag1*", MetadataSearchTargetType.ALL);
    Assert.assertEquals(3, results.size());

    // Try to search for tag1
    results = dataset.searchByKeyValue("ns1", "tags:tag1", MetadataSearchTargetType.ALL);
    Assert.assertEquals(2, results.size());

    // Try to search for tag4
    results = dataset.searchByKeyValue("ns1", "tags:tag4", MetadataSearchTargetType.ALL);
    Assert.assertEquals(1, results.size());

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
    MetadataEntry record = new MetadataEntry(flow1, "key1", "value1");
    // Save it
    dataset.setProperty(flow1, "key1", "value1");

    // Save it
    dataset.setProperty(flow1, "key2", "value2");

    // Search for it based on value
    List<MetadataEntry> results =
      dataset.searchByValue("ns1", "value1", MetadataSearchTargetType.PROGRAM);

    // Assert check
    Assert.assertEquals(1, results.size());

    MetadataEntry result = results.get(0);
    Assert.assertEquals(record, result);

    // Case insensitive
    results = dataset.searchByValue("ns1", "ValUe1", MetadataSearchTargetType.PROGRAM);

    // Assert check
    Assert.assertEquals(1, results.size());

    result = results.get(0);
    Assert.assertEquals(record, result);

    // Save it
    dataset.setProperty(flow1, "key3", "value1");

    // Search for it based on value
    List<MetadataEntry> results2 =
      dataset.searchByValue("ns1", "value1", MetadataSearchTargetType.PROGRAM);

    // Assert check
    Assert.assertEquals(2, results2.size());

    for (MetadataEntry result2 : results2) {
      Assert.assertEquals("value1", result2.getValue());
    }

    // Save it
    dataset.setProperty(stream1, "key21", "value21");

    // Search for it based on value asterix
    List<MetadataEntry> results3 = dataset.searchByValue("ns1", "value2*",
                                                         MetadataSearchTargetType.ALL);

    // Assert check
    Assert.assertEquals(2, results3.size());
    for (MetadataEntry result3 : results3) {
      Assert.assertTrue(result3.getValue().startsWith("value2"));
    }

    // Search for it based on value asterix
    List<MetadataEntry> results4 = dataset.searchByValue("ns12", "value2*",
                                                         MetadataSearchTargetType.ALL);

    // Assert check
    Assert.assertEquals(0, results4.size());
  }

  @Test
  public void testSearchOnKeyValue() throws Exception {
    // Create entry
    MetadataEntry entry = new MetadataEntry(flow1, "key1", "value1");
    // Save it
    dataset.setProperty(flow1, "key1", "value1");

    // Save it
    dataset.setProperty(flow1, "key2", "value2");

    // Search for it based on value
    List<MetadataEntry> results =
      dataset.searchByKeyValue("ns1", "key1" + MetadataDataset.KEYVALUE_SEPARATOR + "value1",
                               MetadataSearchTargetType.PROGRAM);

    // Assert check
    Assert.assertEquals(1, results.size());

    MetadataEntry result = results.get(0);
    Assert.assertEquals(entry, result);

    // Test wrong ns
    List<MetadataEntry> results2  =
      dataset.searchByKeyValue("ns12", "key1" + MetadataDataset.KEYVALUE_SEPARATOR + "value1",
                               MetadataSearchTargetType.PROGRAM);
    // Assert check
    Assert.assertEquals(0, results2.size());
  }

  @Test
  public void testHistory() throws Exception {
    MetadataDataset dataset =
      getDataset(Id.DatasetInstance.from(DatasetFrameworkTestUtil.NAMESPACE_ID, "testHistory"));

    doTestHistory(dataset, flow1, "f_");
    doTestHistory(dataset, app1, "a_");
    doTestHistory(dataset, dataset1, "d_");
    doTestHistory(dataset, stream1, "s_");
  }

  private void doTestHistory(MetadataDataset dataset, Id.NamespacedId targetId, String prefix)
    throws Exception {
    // Metadata change history keyed by time in millis the change was made
    Map<Long, Metadata> expected = new HashMap<>();

    // No history for targetId at the beginning
    Metadata completeRecord = new Metadata(targetId);
    expected.put(System.currentTimeMillis(), completeRecord);
    // Get history for targetId, should be empty
    Assert.assertEquals(ImmutableSet.of(completeRecord),
                        dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), System.currentTimeMillis()));
    // Also, the metadata itself should be equal to the last recorded snapshot
    Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), System.currentTimeMillis())),
                        new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    // Since the key to expected map is time in millis, sleep for a millisecond to make sure the key is distinct
    TimeUnit.MILLISECONDS.sleep(1);

    // Add first record
    completeRecord = new Metadata(targetId, toProps(prefix, "k1", "v1"), toTags(prefix, "t1", "t2"));
    addMetadataHistory(dataset, completeRecord);
    long time = System.currentTimeMillis();
    expected.put(time, completeRecord);
    // Since this is the first record, history should be the same as what was added.
    Assert.assertEquals(ImmutableSet.of(completeRecord),
                        dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
    // Also, the metadata itself should be equal to the last recorded snapshot
    Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), System.currentTimeMillis())),
                        new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    TimeUnit.MILLISECONDS.sleep(1);

    // Add a new property and a tag
    dataset.setProperty(targetId, prefix + "k2", "v2");
    dataset.addTags(targetId, prefix + "t3");
    // Save the complete metadata record at this point
    completeRecord = new Metadata(targetId, toProps(prefix, "k1", "v1", "k2", "v2"),
                                              toTags(prefix, "t1", "t2", "t3"));
    time = System.currentTimeMillis();
    expected.put(time, completeRecord);
    // Assert the history record with the change
    Assert.assertEquals(ImmutableSet.of(completeRecord),
                        dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
    // Also, the metadata itself should be equal to the last recorded snapshot
    Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), System.currentTimeMillis())),
                        new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    TimeUnit.MILLISECONDS.sleep(1);

    // Add another property and a tag
    dataset.setProperty(targetId, prefix + "k3", "v3");
    dataset.addTags(targetId, prefix + "t4");
    // Save the complete metadata record at this point
    completeRecord = new Metadata(targetId, toProps(prefix, "k1", "v1", "k2", "v2", "k3", "v3"),
                                        toTags(prefix, "t1", "t2", "t3", "t4"));
    time = System.currentTimeMillis();
    expected.put(time, completeRecord);
    // Assert the history record with the change
    Assert.assertEquals(ImmutableSet.of(completeRecord),
                        dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
    // Also, the metadata itself should be equal to the last recorded snapshot
    Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), System.currentTimeMillis())),
                        new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    TimeUnit.MILLISECONDS.sleep(1);

    // Add the same property and tag as second time
    dataset.setProperty(targetId, prefix + "k2", "v2");
    dataset.addTags(targetId, prefix + "t3");
    // Save the complete metadata record at this point
    completeRecord = new Metadata(targetId, toProps(prefix, "k1", "v1", "k2", "v2", "k3", "v3"),
                                        toTags(prefix, "t1", "t2", "t3", "t4"));
    time = System.currentTimeMillis();
    expected.put(time, completeRecord);
    // Assert the history record with the change
    Assert.assertEquals(ImmutableSet.of(completeRecord),
                        dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
    // Also, the metadata itself should be equal to the last recorded snapshot
    Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), System.currentTimeMillis())),
                        new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    TimeUnit.MILLISECONDS.sleep(1);

    // Remove a property and two tags
    dataset.removeProperties(targetId, prefix + "k2");
    dataset.removeTags(targetId, prefix + "t4");
    dataset.removeTags(targetId, prefix + "t2");
    // Save the complete metadata record at this point
    completeRecord = new Metadata(targetId, toProps(prefix, "k1", "v1", "k3", "v3"),
                                        toTags(prefix, "t1", "t3"));
    time = System.currentTimeMillis();
    expected.put(time, completeRecord);
    // Assert the history record with the change
    Assert.assertEquals(ImmutableSet.of(completeRecord),
                        dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
    // Also, the metadata itself should be equal to the last recorded snapshot
    Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), System.currentTimeMillis())),
                        new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    TimeUnit.MILLISECONDS.sleep(1);

    // Remove all properties and all tags
    dataset.removeProperties(targetId);
    dataset.removeTags(targetId);
    // Save the complete metadata record at this point
    completeRecord = new Metadata(targetId);
    time = System.currentTimeMillis();
    expected.put(time, completeRecord);
    // Assert the history record with the change
    Assert.assertEquals(ImmutableSet.of(completeRecord),
                        dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
    // Also, the metadata itself should be equal to the last recorded snapshot
    Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), System.currentTimeMillis())),
                        new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    TimeUnit.MILLISECONDS.sleep(1);

    // Add one more property and a tag
    dataset.setProperty(targetId, prefix + "k2", "v2");
    dataset.addTags(targetId, prefix + "t2");
    // Save the complete metadata record at this point
    completeRecord = new Metadata(targetId, toProps(prefix, "k2", "v2"),
                                        toTags(prefix, "t2"));
    time = System.currentTimeMillis();
    expected.put(time, completeRecord);
    // Assert the history record with the change
    Assert.assertEquals(ImmutableSet.of(completeRecord),
                        dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
    // Also, the metadata itself should be equal to the last recorded snapshot
    Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), System.currentTimeMillis())),
                        new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    TimeUnit.MILLISECONDS.sleep(1);

    // Now assert all history
    for (Map.Entry<Long, Metadata> entry : expected.entrySet()) {
      Assert.assertEquals(entry.getValue(),
                          getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), entry.getKey())));
    }

    // Asserting for current time should give the latest record
    Assert.assertEquals(ImmutableSet.of(completeRecord),
                        dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), System.currentTimeMillis()));
    // Also, the metadata itself should be equal to the last recorded snapshot
    Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), System.currentTimeMillis())),
                        new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
  }

  private void addMetadataHistory(MetadataDataset dataset, Metadata record) {
    for (Map.Entry<String, String> entry : record.getProperties().entrySet()) {
      dataset.setProperty(record.getEntityId(), entry.getKey(), entry.getValue());
    }
    //noinspection ToArrayCallWithZeroLengthArrayArgument
    dataset.addTags(record.getEntityId(), record.getTags().toArray(new String[0]));
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

  private <T> T getFirst(Iterable<T> iterable) {
    Assert.assertEquals(1, Iterables.size(iterable));
    return iterable.iterator().next();
  }

  private static MetadataDataset getDataset(Id.DatasetInstance instance) throws Exception {
    return DatasetsUtil.getOrCreateDataset(dsFrameworkUtil.getFramework(), instance,
                                           MetadataDataset.class.getName(),
                                           DatasetProperties.EMPTY, null, null);
  }
}
