/*
 * Copyright 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import co.cask.cdap.data2.metadata.indexer.Indexer;
import co.cask.cdap.data2.metadata.indexer.InvertedValueIndexer;
import co.cask.cdap.data2.metadata.system.AbstractSystemMetadataWriter;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionFailureException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test class for {@link MetadataDataset} class.
 */
public class  MetadataDatasetTest {

  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  private static final DatasetId datasetInstance = DatasetFrameworkTestUtil.NAMESPACE_ID.dataset("meta");

  private MetadataDataset dataset;
  private TransactionExecutor txnl;

  private final MetadataEntity app1 = new ApplicationId("ns1", "app1").toMetadataEntity();
  private final MetadataEntity appNs2 = new ApplicationId("ns2", "app1").toMetadataEntity();
  // Have to use Id.Program for comparison here because the MetadataDataset APIs return Id.Program.
  private final MetadataEntity flow1 = new ProgramId("ns1", "app1", ProgramType.FLOW, "flow1").toMetadataEntity();
  private final MetadataEntity dataset1 = new DatasetId("ns1", "ds1").toMetadataEntity();
  private final MetadataEntity stream1 = new StreamId("ns1", "s1").toMetadataEntity();
  private final MetadataEntity view1 = new StreamViewId("ns1", "s1", "v1").toMetadataEntity();
  private final MetadataEntity artifact1 = new ArtifactId("ns1", "a1", "1.0.0").toMetadataEntity();
  private final MetadataEntity fileEntity = MetadataEntity.ofDataset("ns1", "ds1").append("file", "f1");
  private final MetadataEntity partitionFileEntity = MetadataEntity.ofDataset("ns1", "ds1").append("partition", "p1")
    .append("file", "f1");
  private final MetadataEntity jarEntity = MetadataEntity.ofNamespace("ns1").append("jar", "jar1");

  @Before
  public void before() throws Exception {
    dataset = getDataset(datasetInstance);
    txnl = dsFrameworkUtil.newInMemoryTransactionExecutor((TransactionAware) dataset);
  }

  @After
  public void after() throws Exception {
    dataset = null;
    DatasetAdmin admin = dsFrameworkUtil.getFramework().getAdmin(datasetInstance, null);
    if (admin != null) {
      admin.truncate();
    }
  }

  @Test
  public void testProperties() throws Exception {
    txnl.execute(() -> {
      Assert.assertEquals(0, dataset.getProperties(app1).size());
      Assert.assertEquals(0, dataset.getProperties(flow1).size());
      Assert.assertEquals(0, dataset.getProperties(dataset1).size());
      Assert.assertEquals(0, dataset.getProperties(stream1).size());
      Assert.assertEquals(0, dataset.getProperties(view1).size());
      Assert.assertEquals(0, dataset.getProperties(artifact1).size());
      Assert.assertEquals(0, dataset.getProperties(fileEntity).size());
      Assert.assertEquals(0, dataset.getProperties(partitionFileEntity).size());
      Assert.assertEquals(0, dataset.getProperties(jarEntity).size());
    });
    // Set some properties
    txnl.execute(() -> {
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
      dataset.setProperty(fileEntity, "fkey2", "fvalue2");
      dataset.setProperty(partitionFileEntity, "pfkey2", "pfvalue2");
      dataset.setProperty(jarEntity, "jkey2", "jvalue2");
    });
    // verify
    txnl.execute(() -> {
      Map<String, String> properties = dataset.getProperties(app1);
      Assert.assertEquals(ImmutableMap.of("akey1", "avalue1"), properties);
    });
    txnl.execute(() -> dataset.removeProperties(app1, "akey1"));
    txnl.execute(() -> {
      Map<String, String> properties = dataset.getProperties(jarEntity);
      Assert.assertEquals(ImmutableMap.of("jkey2", "jvalue2"), properties);
    });
    txnl.execute(() -> dataset.removeProperties(jarEntity, "jkey2"));

    txnl.execute(() -> {
      Assert.assertNull(dataset.getProperty(app1, "akey1"));
      MetadataEntry result = dataset.getProperty(flow1, "fkey1");
      MetadataEntry expected = new MetadataEntry(flow1, "fkey1", "fvalue1");
      Assert.assertEquals(expected, result);
      Assert.assertEquals(ImmutableMap.of("fkey1", "fvalue1", "fK", "fV"), dataset.getProperties(flow1));
    });
    txnl.execute(() -> dataset.removeProperties(flow1, "fkey1"));
    txnl.execute(() -> {
      Map<String, String> properties = dataset.getProperties(flow1);
      Assert.assertEquals(1, properties.size());
      Assert.assertEquals("fV", properties.get("fK"));
    });
    txnl.execute(() -> dataset.removeProperties(flow1));
    txnl.execute(() -> {
      Assert.assertEquals(0, dataset.getProperties(flow1).size());
      Assert.assertEquals(0, dataset.getProperties(jarEntity).size());
      MetadataEntry expected = new MetadataEntry(dataset1, "dkey1", "dvalue1");
      Assert.assertEquals(expected, dataset.getProperty(dataset1, "dkey1"));
      Assert.assertEquals(ImmutableMap.of("skey1", "svalue1", "skey2", "svalue2"), dataset.getProperties(stream1));
      Map<String, String> properties = dataset.getProperties(artifact1);
      Assert.assertEquals(ImmutableMap.of("rkey1", "rvalue1", "rkey2", "rvalue2"), properties);
      MetadataEntry result = dataset.getProperty(artifact1, "rkey2");
      expected = new MetadataEntry(artifact1, "rkey2", "rvalue2");
      Assert.assertEquals(expected, result);
      properties = dataset.getProperties(view1);
      Assert.assertEquals(ImmutableMap.of("vkey1", "vvalue1", "vkey2", "vvalue2"), properties);
      result = dataset.getProperty(view1, "vkey2");
      expected = new MetadataEntry(view1, "vkey2", "vvalue2");
      Assert.assertEquals(expected, result);
      result = dataset.getProperty(fileEntity, "fkey2");
      expected = new MetadataEntry(fileEntity, "fkey2", "fvalue2");
      Assert.assertEquals(expected, result);
      result = dataset.getProperty(partitionFileEntity, "pfkey2");
      expected = new MetadataEntry(partitionFileEntity, "pfkey2", "pfvalue2");
      Assert.assertEquals(expected, result);
    });
    // reset a property
    txnl.execute(() -> dataset.setProperty(stream1, "skey1", "sv1"));
    txnl.execute(() -> Assert.assertEquals(ImmutableMap.of("skey1", "sv1", "skey2", "svalue2"),
                                           dataset.getProperties(stream1)));
    // cleanup
    txnl.execute(() -> {
      dataset.removeProperties(app1);
      dataset.removeProperties(flow1);
      dataset.removeProperties(dataset1);
      dataset.removeProperties(stream1);
      dataset.removeProperties(artifact1);
      dataset.removeProperties(view1);
      dataset.removeProperties(fileEntity);
      dataset.removeProperties(partitionFileEntity);
    });
    txnl.execute(() -> {
      Assert.assertEquals(0, dataset.getProperties(app1).size());
      Assert.assertEquals(0, dataset.getProperties(flow1).size());
      Assert.assertEquals(0, dataset.getProperties(dataset1).size());
      Assert.assertEquals(0, dataset.getProperties(stream1).size());
      Assert.assertEquals(0, dataset.getProperties(view1).size());
      Assert.assertEquals(0, dataset.getProperties(artifact1).size());
      Assert.assertEquals(0, dataset.getProperties(fileEntity).size());
      Assert.assertEquals(0, dataset.getProperties(partitionFileEntity).size());
      Assert.assertEquals(0, dataset.getProperties(jarEntity).size());
    });
  }

  @Test
  public void testTags() throws InterruptedException, TransactionFailureException {
    txnl.execute(() -> {
      Assert.assertEquals(0, dataset.getTags(app1).size());
      Assert.assertEquals(0, dataset.getTags(flow1).size());
      Assert.assertEquals(0, dataset.getTags(dataset1).size());
      Assert.assertEquals(0, dataset.getTags(stream1).size());
      Assert.assertEquals(0, dataset.getTags(view1).size());
      Assert.assertEquals(0, dataset.getTags(artifact1).size());
      Assert.assertEquals(0, dataset.getTags(fileEntity).size());
      Assert.assertEquals(0, dataset.getTags(partitionFileEntity).size());
      Assert.assertEquals(0, dataset.getTags(jarEntity).size());
    });
    txnl.execute(() -> {
      dataset.addTags(app1, "tag1", "tag2", "tag3");
      dataset.addTags(flow1, "tag1");
      dataset.addTags(dataset1, "tag3", "tag2");
      dataset.addTags(stream1, "tag2");
      dataset.addTags(view1, "tag4");
      dataset.addTags(artifact1, "tag3");
      dataset.addTags(fileEntity, "tag5");
      dataset.addTags(partitionFileEntity, "tag6");
      dataset.addTags(jarEntity, "tag5", "tag7");
    });
    txnl.execute(() -> {
      Set<String> tags = dataset.getTags(app1);
      Assert.assertEquals(3, tags.size());
      Assert.assertTrue(tags.contains("tag1"));
      Assert.assertTrue(tags.contains("tag2"));
      Assert.assertTrue(tags.contains("tag3"));
    });
    // add the same tag again
    txnl.execute(() -> dataset.addTags(app1, "tag1"));
    txnl.execute(() -> {
      Assert.assertEquals(3, dataset.getTags(app1).size());
      Set<String> tags = dataset.getTags(flow1);
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
      tags = dataset.getTags(fileEntity);
      Assert.assertEquals(1, tags.size());
      Assert.assertTrue(tags.contains("tag5"));
      tags = dataset.getTags(partitionFileEntity);
      Assert.assertEquals(1, tags.size());
      Assert.assertTrue(tags.contains("tag6"));
      tags = dataset.getTags(jarEntity);
      Assert.assertEquals(2, tags.size());
      Assert.assertTrue(tags.contains("tag5"));
      Assert.assertTrue(tags.contains("tag7"));
    });
    txnl.execute(() -> dataset.removeTags(app1, "tag1", "tag2"));
    txnl.execute(() -> {
      Set<String> tags = dataset.getTags(app1);
      Assert.assertEquals(1, tags.size());
      Assert.assertTrue(tags.contains("tag3"));
    });
    txnl.execute(() -> dataset.removeTags(dataset1, "tag3"));
    txnl.execute(() -> dataset.removeTags(jarEntity, "tag5"));
    txnl.execute(() -> {
      Set<String> tags = dataset.getTags(dataset1);
      Assert.assertEquals(1, tags.size());
      Assert.assertTrue(tags.contains("tag2"));
      tags = dataset.getTags(artifact1);
      Assert.assertEquals(1, tags.size());
      Assert.assertTrue(tags.contains("tag3"));
      tags = dataset.getTags(jarEntity);
      Assert.assertEquals(1, tags.size());
      Assert.assertTrue(tags.contains("tag7"));
    });
    // cleanup
    txnl.execute(() -> {
      dataset.removeTags(app1);
      dataset.removeTags(flow1);
      dataset.removeTags(dataset1);
      dataset.removeTags(stream1);
      dataset.removeTags(view1);
      dataset.removeTags(artifact1);
      dataset.removeTags(fileEntity);
      dataset.removeTags(partitionFileEntity);
      dataset.removeTags(jarEntity);
    });
    txnl.execute(() -> {
      Assert.assertEquals(0, dataset.getTags(app1).size());
      Assert.assertEquals(0, dataset.getTags(flow1).size());
      Assert.assertEquals(0, dataset.getTags(dataset1).size());
      Assert.assertEquals(0, dataset.getTags(stream1).size());
      Assert.assertEquals(0, dataset.getTags(view1).size());
      Assert.assertEquals(0, dataset.getTags(artifact1).size());
      Assert.assertEquals(0, dataset.getTags(fileEntity).size());
      Assert.assertEquals(0, dataset.getTags(partitionFileEntity).size());
      Assert.assertEquals(0, dataset.getTags(jarEntity).size());
    });
  }

  @Test
  public void testSearchOnTags() throws Exception {
    txnl.execute(() -> {
      Assert.assertEquals(0, dataset.getTags(app1).size());
      Assert.assertEquals(0, dataset.getTags(appNs2).size());
      Assert.assertEquals(0, dataset.getTags(flow1).size());
      Assert.assertEquals(0, dataset.getTags(dataset1).size());
      Assert.assertEquals(0, dataset.getTags(stream1).size());
      Assert.assertEquals(0, dataset.getTags(fileEntity).size());
      dataset.addTags(app1, "tag1", "tag2", "tag3");
      dataset.addTags(appNs2, "tag1", "tag2", "tag3_more");
      dataset.addTags(flow1, "tag1");
      dataset.addTags(dataset1, "tag3", "tag2", "tag12-tag33");
      dataset.addTags(stream1, "tag2, tag4");
      dataset.addTags(fileEntity, "tag2, tag5");
    });

    // Try to search on all tags
    txnl.execute(() -> {
      List<MetadataEntry> results =
        searchByDefaultIndex("ns1", "tags:*", ImmutableSet.of(EntityTypeSimpleName.ALL));
      // results tags in ns1 are app1-tag1, tag2, tag3 + flow1-tag1 + dataset1-tag3, tag2, tag12-tag33, tag12, tag33
      // + stream1-tag2, tag4 + fileEntity-tag2, tag5
      Assert.assertEquals(13, results.size());

      // Try to search for tag1*
      results = searchByDefaultIndex("ns1", "tags:tag1*", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertEquals(4, results.size());

      // Try to search for tag1 with spaces in search query and mixed case of tags keyword
      results = searchByDefaultIndex("ns1", "  tAGS  :  tag1  ", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertEquals(2, results.size());

      // Try to search for tag5
      results = searchByDefaultIndex("ns1", "tags:tag5", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertEquals(1, results.size());

      // Try to search for tag2
      results = searchByDefaultIndex("ns1", "tags:tag2", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertEquals(4, results.size());

      // Try to search for tag4
      results = searchByDefaultIndex("ns1", "tags:tag4", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertEquals(1, results.size());

      // Try to search for tag33
      results = searchByDefaultIndex("ns1", "tags:tag33", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertEquals(1, results.size());

      // Try to search for a tag which has - in it
      results = searchByDefaultIndex("ns1", "tag12-tag33", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertEquals(1, results.size());

      // Try to search for tag33 with spaces in query
      results = searchByDefaultIndex("ns1", "  tag33  ", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertEquals(1, results.size());

      // Try to search for tag3
      results = searchByDefaultIndex("ns1", "tags:tag3*", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertEquals(3, results.size());

      // try search in another namespace
      results = searchByDefaultIndex("ns2", "tags:tag1", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertEquals(1, results.size());

      results = searchByDefaultIndex("ns2", "tag3", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertEquals(1, results.size());

      results = searchByDefaultIndex("ns2", "tag*", ImmutableSet.of(EntityTypeSimpleName.APP));
      // 9 due to matches of type ns2:tag1, ns2:tags:tag1, and splitting of tag3_more
      Assert.assertEquals(9, results.size());

    });
    // cleanup
    txnl.execute(() -> {
      dataset.removeTags(app1);
      dataset.removeTags(flow1);
      dataset.removeTags(dataset1);
      dataset.removeTags(stream1);
    });
    // Search should be empty after deleting tags
    txnl.execute(() -> {
      List<MetadataEntry> results =
        searchByDefaultIndex("ns1", "tags:tag3*", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertEquals(0, results.size());
      Assert.assertEquals(0, dataset.getTags(app1).size());
      Assert.assertEquals(0, dataset.getTags(flow1).size());
      Assert.assertEquals(0, dataset.getTags(dataset1).size());
      Assert.assertEquals(0, dataset.getTags(stream1).size());
    });
  }

  @Test
  public void testSearchOnValue() throws Exception {
    // Add some metadata
    final MetadataEntry entry = new MetadataEntry(flow1, "key1", "value1");
    final String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";
    final MetadataEntry multiWordEntry = new MetadataEntry(flow1, "multiword", multiWordValue);

    txnl.execute(() -> {
      dataset.setProperty(flow1, "key1", "value1");
      dataset.setProperty(flow1, "key2", "value2");
      dataset.setProperty(flow1, "multiword", multiWordValue);
    });

    // Search for it based on value
    txnl.execute(() -> {
      List<MetadataEntry> results =
        searchByDefaultIndex("ns1", "value1", ImmutableSet.of(EntityTypeSimpleName.PROGRAM));
      Assert.assertEquals(ImmutableList.of(entry), results);

      // Search for it based on a word in value with spaces in search query
      results = searchByDefaultIndex("ns1", "  aV1   ", ImmutableSet.of(EntityTypeSimpleName.PROGRAM));
      Assert.assertEquals(ImmutableList.of(multiWordEntry), results);

      // Search for it based split patterns to make sure nothing is matched
      results = searchByDefaultIndex("ns1", "-", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertTrue(results.isEmpty());
      results = searchByDefaultIndex("ns1", ",", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertTrue(results.isEmpty());
      results = searchByDefaultIndex("ns1", "_", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertTrue(results.isEmpty());
      results = searchByDefaultIndex("ns1", ", ,", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertTrue(results.isEmpty());
      results = searchByDefaultIndex("ns1", ", - ,", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertTrue(results.isEmpty());

      // Search for it based on a word in value
      results = searchByDefaultIndex("ns1", "av5", ImmutableSet.of(EntityTypeSimpleName.PROGRAM));
      Assert.assertEquals(ImmutableList.of(multiWordEntry), results);

      // Case insensitive
      results = searchByDefaultIndex("ns1", "ValUe1", ImmutableSet.of(EntityTypeSimpleName.PROGRAM));
      Assert.assertEquals(ImmutableList.of(entry), results);

    });
    // Search based on value
    txnl.execute(() -> dataset.setProperty(flow1, "key3", "value1"));
    txnl.execute(() -> {
      List<MetadataEntry> results =
        searchByDefaultIndex("ns1", "value1", ImmutableSet.of(EntityTypeSimpleName.PROGRAM));
      Assert.assertEquals(2, results.size());
      for (MetadataEntry result : results) {
        Assert.assertEquals("value1", result.getValue());
      }
    });

    // Search based on value prefix
    txnl.execute(() -> dataset.setProperty(stream1, "key21", "value21"));
    txnl.execute(() -> {
      List<MetadataEntry> results =
        searchByDefaultIndex("ns1", "value2*", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertEquals(2, results.size());
      for (MetadataEntry result : results) {
        Assert.assertTrue(result.getValue().startsWith("value2"));
      }
      // Search based on value prefix in the wrong namespace
      results = searchByDefaultIndex("ns12", "value2*", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertTrue(results.isEmpty());
    });

  }

  @Test
  public void testSearchOnKeyValue() throws Exception {
    final MetadataEntry flowEntry1 = new MetadataEntry(flow1, "key1", "value1");
    final MetadataEntry flowEntry2 = new MetadataEntry(flow1, "key2", "value2");
    final String multiWordKey = "multiword";
    final String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";
    final MetadataEntry streamEntry1 = new MetadataEntry(stream1, "Key1", "Value1");
    final MetadataEntry streamEntry2 = new MetadataEntry(stream1, "sKey1", "sValue1");

    txnl.execute(() -> {
      // Add some properties to flow1
      dataset.setProperty(flow1, "key1", "value1");
      dataset.setProperty(flow1, "key2", "value2");
      // add a multi word value
      dataset.setProperty(flow1, multiWordKey, multiWordValue);
      dataset.setProperty(stream1, "sKey1", "sValue1");
      dataset.setProperty(stream1, "Key1", "Value1");
    });

    txnl.execute(() -> {
      // Search for it based on value
      List<MetadataEntry> results =
        searchByDefaultIndex("ns1", "key1" + MetadataDataset.KEYVALUE_SEPARATOR + "value1",
                             ImmutableSet.of(EntityTypeSimpleName.PROGRAM));
      Assert.assertEquals(ImmutableList.of(flowEntry1), results);

      // Search for it based on a word in value with spaces in search query
      results = searchByDefaultIndex("ns1", "  multiword" + MetadataDataset.KEYVALUE_SEPARATOR + "aV1   ",
                                     ImmutableSet.of(EntityTypeSimpleName.PROGRAM));

      MetadataEntry flowMultiWordEntry = new MetadataEntry(flow1, multiWordKey, multiWordValue);
      Assert.assertEquals(ImmutableList.of(flowMultiWordEntry), results);

      // Search for it based on a word in value
      results =
        searchByDefaultIndex("ns1", multiWordKey + MetadataDataset.KEYVALUE_SEPARATOR + "aV5",
                             ImmutableSet.of(EntityTypeSimpleName.PROGRAM));
      Assert.assertEquals(ImmutableList.of(flowMultiWordEntry), results);
    });
    txnl.execute(() -> dataset.removeProperties(flow1, multiWordKey));
    txnl.execute(() -> {
      List<MetadataEntry> results =
        searchByDefaultIndex("ns1", multiWordKey + MetadataDataset.KEYVALUE_SEPARATOR + "aV5",
                             ImmutableSet.of(EntityTypeSimpleName.PROGRAM));
      // search results should be empty after removing this key as the indexes are deleted
      Assert.assertTrue(results.isEmpty());

      // Test wrong ns
      List<MetadataEntry> results2 =
        searchByDefaultIndex("ns12", "key1" + MetadataDataset.KEYVALUE_SEPARATOR + "value1",
                             ImmutableSet.of(EntityTypeSimpleName.PROGRAM));
      Assert.assertTrue(results2.isEmpty());

      // Test multi word query
      results = searchByDefaultIndex("ns1", "  value1  av2 ", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertEquals(Sets.newHashSet(flowEntry1, streamEntry1), Sets.newHashSet(results));

      results = searchByDefaultIndex("ns1", "  value1  sValue1 ", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertEquals(Sets.newHashSet(flowEntry1, streamEntry1, streamEntry2), Sets.newHashSet(results));

      results = searchByDefaultIndex("ns1", "  valu*  sVal* ", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertEquals(Sets.newHashSet(flowEntry1, flowEntry2, streamEntry1, streamEntry2),
                          Sets.newHashSet(results));

      // Using empty filter should also search for all target types
      results = searchByDefaultIndex("ns1", "  valu*  sVal* ", ImmutableSet.<EntityTypeSimpleName>of());
      Assert.assertEquals(Sets.newHashSet(flowEntry1, flowEntry2, streamEntry1, streamEntry2),
                          Sets.newHashSet(results));
    });

  }

  @Test
  public void testSearchIncludesSystemEntities() throws InterruptedException, TransactionFailureException {
    // Use the same artifact in two different namespaces - system and ns2
    final MetadataEntity sysArtifact = NamespaceId.SYSTEM.artifact("artifact", "1.0").toMetadataEntity();
    final MetadataEntity ns2Artifact = new ArtifactId("ns2", "artifact", "1.0").toMetadataEntity();
    final String multiWordKey = "multiword";
    final String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";

    txnl.execute(() -> {
      dataset.setProperty(flow1, multiWordKey, multiWordValue);
      dataset.setProperty(sysArtifact, multiWordKey, multiWordValue);
      dataset.setProperty(ns2Artifact, multiWordKey, multiWordValue);
    });
    // perform the exact same multiword search in the 'ns1' namespace. It should return the system artifact along with
    // matched entities in the 'ns1' namespace
    final MetadataEntry flowMultiWordEntry = new MetadataEntry(flow1, multiWordKey, multiWordValue);
    final MetadataEntry systemArtifactEntry = new MetadataEntry(sysArtifact, multiWordKey, multiWordValue);
    final MetadataEntry ns2ArtifactEntry = new MetadataEntry(ns2Artifact, multiWordKey, multiWordValue);

    txnl.execute(() -> {
      List<MetadataEntry> results = searchByDefaultIndex("ns1", "aV5", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertEquals(Sets.newHashSet(flowMultiWordEntry, systemArtifactEntry), Sets.newHashSet(results));
      // search only programs - should only return flow
      results = searchByDefaultIndex("ns1", multiWordKey + MetadataDataset.KEYVALUE_SEPARATOR + "aV5",
                                     ImmutableSet.of(EntityTypeSimpleName.PROGRAM));
      Assert.assertEquals(ImmutableList.of(flowMultiWordEntry), results);
      // search only artifacts - should only return system artifact
      results = searchByDefaultIndex("ns1", multiWordKey + MetadataDataset.KEYVALUE_SEPARATOR + multiWordValue,
                                     ImmutableSet.of(EntityTypeSimpleName.ARTIFACT));
      // this query returns the system artifact 4 times, since the dataset returns a list with duplicates for scoring
      // purposes. Convert to a Set for comparison.
      Assert.assertEquals(Sets.newHashSet(systemArtifactEntry), Sets.newHashSet(results));
      // search all entities in namespace 'ns2' - should return the system artifact and the same artifact in ns2
      results = searchByDefaultIndex("ns2", multiWordKey + MetadataDataset.KEYVALUE_SEPARATOR + "aV4",
                                     ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertEquals(Sets.newHashSet(systemArtifactEntry, ns2ArtifactEntry), Sets.newHashSet(results));
      // search only programs in a namespace 'ns2'. Should return empty
      results = searchByDefaultIndex("ns2", "aV*", ImmutableSet.of(EntityTypeSimpleName.PROGRAM));
      Assert.assertTrue(results.isEmpty());
      // search all entities in namespace 'ns3'. Should return only the system artifact
      results = searchByDefaultIndex("ns3", "av*", ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertEquals(Sets.newHashSet(systemArtifactEntry), Sets.newHashSet(results));
      // search the system namespace for all entities. Should return only the system artifact
      results = searchByDefaultIndex(NamespaceId.SYSTEM.getEntityName(), "av*",
                                     ImmutableSet.of(EntityTypeSimpleName.ALL));
      Assert.assertEquals(Sets.newHashSet(systemArtifactEntry), Sets.newHashSet(results));
    });
    // clean up
    txnl.execute(() -> {
      dataset.removeProperties(flow1);
      dataset.removeProperties(sysArtifact);
    });
  }

  @Test
  public void testSearchDifferentEntityScope() throws InterruptedException, TransactionFailureException {
    final MetadataEntity sysArtifact = NamespaceId.SYSTEM.artifact("artifact", "1.0").toMetadataEntity();
    final MetadataEntity nsArtifact = new ArtifactId("ns1", "artifact", "1.0").toMetadataEntity();
    final String multiWordKey = "multiword";
    final String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";

    txnl.execute(() -> {
      dataset.setProperty(nsArtifact, multiWordKey, multiWordValue);
      dataset.setProperty(sysArtifact, multiWordKey, multiWordValue);
    });

    final MetadataEntry systemArtifactEntry = new MetadataEntry(sysArtifact, multiWordKey, multiWordValue);
    final MetadataEntry nsArtifactEntry = new MetadataEntry(nsArtifact, multiWordKey, multiWordValue);

    txnl.execute(() -> {
      List<MetadataEntry> results = dataset.search("ns1", "aV5", ImmutableSet.of(EntityTypeSimpleName.ALL),
                                                   SortInfo.DEFAULT, 0, Integer.MAX_VALUE, 1, null,
                                                   false, EnumSet.of(EntityScope.USER)).getResults();
      // the result should not contain system entities
      Assert.assertEquals(Sets.newHashSet(nsArtifactEntry), Sets.newHashSet(results));
      results = dataset.search("ns1", "aV5", ImmutableSet.of(EntityTypeSimpleName.ALL),
                               SortInfo.DEFAULT, 0, Integer.MAX_VALUE, 1, null,
                               false, EnumSet.of(EntityScope.SYSTEM)).getResults();
      // the result should not contain user entities
      Assert.assertEquals(Sets.newHashSet(systemArtifactEntry), Sets.newHashSet(results));
      results = dataset.search("ns1", "aV5", ImmutableSet.of(EntityTypeSimpleName.ALL),
                               SortInfo.DEFAULT, 0, Integer.MAX_VALUE, 1, null,
                               false, EnumSet.allOf(EntityScope.class)).getResults();
      // the result should contain both entity scopes
      Assert.assertEquals(Sets.newHashSet(nsArtifactEntry, systemArtifactEntry), Sets.newHashSet(results));
    });

    // clean up
    txnl.execute(() -> {
      dataset.removeProperties(nsArtifact);
      dataset.removeProperties(sysArtifact);
    });
  }

  @Test
  public void testUpdateSearch() throws Exception {
    txnl.execute(() -> {
      dataset.setProperty(flow1, "key1", "value1");
      dataset.setProperty(flow1, "key2", "value2");
      dataset.addTags(flow1, "tag1", "tag2");
    });
    txnl.execute(() -> {
      Assert.assertEquals(ImmutableList.of(new MetadataEntry(flow1, "key1", "value1")),
                          searchByDefaultIndex(flow1.getValue(MetadataEntity.NAMESPACE), "value1",
                                               ImmutableSet.<EntityTypeSimpleName>of()));
      Assert.assertEquals(ImmutableList.of(new MetadataEntry(flow1, "key2", "value2")),
                          searchByDefaultIndex(flow1.getValue(MetadataEntity.NAMESPACE), "value2",
                                               ImmutableSet.<EntityTypeSimpleName>of()));
      Assert.assertEquals(ImmutableList.of(new MetadataEntry(flow1, MetadataDataset.TAGS_KEY, "tag1,tag2")),
                          searchByDefaultIndex(flow1.getValue(MetadataEntity.NAMESPACE), "tag2",
                                               ImmutableSet.<EntityTypeSimpleName>of()));
    });

    // Update key1
    txnl.execute(() -> {
      dataset.setProperty(flow1, "key1", "value3");
      dataset.removeProperties(flow1, "key2");
      dataset.removeTags(flow1, "tag2");
    });

    txnl.execute(() -> {
      // Searching for value1 should be empty
      Assert.assertEquals(ImmutableList.of(),
                          searchByDefaultIndex(flow1.getValue(MetadataEntity.NAMESPACE), "value1",
                                               ImmutableSet.<EntityTypeSimpleName>of()));
      // Instead key1 has value value3 now
      Assert.assertEquals(ImmutableList.of(new MetadataEntry(flow1, "key1", "value3")),
                          searchByDefaultIndex(flow1.getValue(MetadataEntity.NAMESPACE), "value3",
                                               ImmutableSet.<EntityTypeSimpleName>of()));
      // key2 was deleted
      Assert.assertEquals(ImmutableList.of(),
                          searchByDefaultIndex(flow1.getValue(MetadataEntity.NAMESPACE), "value2",
                                               ImmutableSet.<EntityTypeSimpleName>of()));
      // tag2 was deleted
      Assert.assertEquals(
        ImmutableList.of(),
        searchByDefaultIndex(flow1.getValue(MetadataEntity.NAMESPACE), "tag2", ImmutableSet.of()))
      ;
      Assert.assertEquals(
        ImmutableList.of(new MetadataEntry(flow1, MetadataDataset.TAGS_KEY, "tag1")),
        searchByDefaultIndex(flow1.getValue(MetadataEntity.NAMESPACE), "tag1", ImmutableSet.of())
      );
    });
  }

  @Test
  public void testMultiGet() throws Exception {
    final Map<MetadataEntity, Metadata> allMetadata = new HashMap<>();
    allMetadata.put(flow1, new Metadata(flow1,
                                        ImmutableMap.of("key1", "value1", "key2", "value2"),
                                        ImmutableSet.of("tag1", "tag2", "tag3")));
    allMetadata.put(dataset1, new Metadata(dataset1,
                                           ImmutableMap.of("key10", "value10", "key11", "value11"),
                                           ImmutableSet.of()));
    allMetadata.put(app1, new Metadata(app1,
                                       ImmutableMap.of("key20", "value20", "key21", "value21"),
                                       ImmutableSet.of()));
    allMetadata.put(stream1, new Metadata(stream1,
                                          ImmutableMap.of("key30", "value30", "key31", "value31", "key32", "value32"),
                                          ImmutableSet.of()));
    allMetadata.put(artifact1, new Metadata(artifact1,
                                            ImmutableMap.of("key40", "value41"),
                                            ImmutableSet.of()));
    allMetadata.put(view1, new Metadata(view1,
                                        ImmutableMap.of("key50", "value50", "key51", "value51"),
                                        ImmutableSet.of("tag51")));

    txnl.execute(() -> {
      for (Map.Entry<MetadataEntity, Metadata> entry : allMetadata.entrySet()) {
        Metadata metadata = entry.getValue();
        for (Map.Entry<String, String> props : metadata.getProperties().entrySet()) {
          dataset.setProperty(metadata.getMetadataEntity(), props.getKey(), props.getValue());
        }
        dataset.addTags(metadata.getMetadataEntity(),
                        metadata.getTags().toArray(new String[metadata.getTags().size()]));
      }
    });

    txnl.execute(() -> {
      ImmutableSet<Metadata> expected =
        ImmutableSet.<Metadata>builder()
          .add(allMetadata.get(flow1))
          .add(allMetadata.get(app1))
          .build();
      Assert.assertEquals(expected, dataset.getMetadata(ImmutableSet.of(flow1, app1)));

      expected =
        ImmutableSet.<Metadata>builder()
          .add(allMetadata.get(view1))
          .add(allMetadata.get(stream1))
          .add(allMetadata.get(dataset1))
          .add(allMetadata.get(artifact1))
          .build();
      Assert.assertEquals(expected, dataset.getMetadata(ImmutableSet.of(view1, stream1, dataset1, artifact1)));

      expected =
        ImmutableSet.<Metadata>builder()
          .add(allMetadata.get(artifact1))
          .build();
      Assert.assertEquals(expected, dataset.getMetadata(ImmutableSet.of(artifact1)));

      expected = ImmutableSet.of();
      Assert.assertEquals(expected, dataset.getMetadata(ImmutableSet.of()));
    });
  }

  @Test
  public void testDelete() throws Exception {
    txnl.execute(() -> {
      dataset.setProperty(flow1, "key1", "value1");
      dataset.setProperty(flow1, "key2", "value2");
      dataset.addTags(flow1, "tag1", "tag2");

      dataset.setProperty(app1, "key10", "value10");
      dataset.setProperty(app1, "key12", "value12");
      dataset.addTags(app1, "tag11", "tag12");
    });

    txnl.execute(() -> {
      Assert.assertEquals(ImmutableMap.of("key1", "value1", "key2", "value2"), dataset.getProperties(flow1));
      Assert.assertEquals(ImmutableSet.of("tag1", "tag2"), dataset.getTags(flow1));
      Assert.assertEquals(ImmutableMap.of("key10", "value10", "key12", "value12"), dataset.getProperties(app1));
      Assert.assertEquals(ImmutableSet.of("tag11", "tag12"), dataset.getTags(app1));
    });

    txnl.execute(() -> {
      // Delete all tags for flow1, and delete all properties for app1
      dataset.removeTags(flow1);
      dataset.removeProperties(app1);
    });

    txnl.execute(() -> {
      Assert.assertEquals(ImmutableMap.of("key1", "value1", "key2", "value2"), dataset.getProperties(flow1));
      Assert.assertEquals(ImmutableSet.of(), dataset.getTags(flow1));
      Assert.assertEquals(ImmutableMap.of(), dataset.getProperties(app1));
      Assert.assertEquals(ImmutableSet.of("tag11", "tag12"), dataset.getTags(app1));
    });
  }

  @Test
  public void testHistory() throws Exception {
    MetadataDataset dataset =
      getDataset(DatasetFrameworkTestUtil.NAMESPACE_ID.dataset("testHistory"));

    doTestHistory(dataset, flow1, "f_");
    doTestHistory(dataset, app1, "a_");
    doTestHistory(dataset, dataset1, "d_");
    doTestHistory(dataset, stream1, "s_");
  }

  @Test
  public void testIndexRebuilding() throws Exception {
    final MetadataDataset dataset =
      getDataset(DatasetFrameworkTestUtil.NAMESPACE_ID.dataset("testIndexRebuilding"));
    TransactionExecutor txnl = dsFrameworkUtil.newInMemoryTransactionExecutor((TransactionAware) dataset);
    txnl.execute(() -> {
      Indexer indexer = new ReversingIndexer();
      dataset.setMetadata(new MetadataEntry(flow1, "flowKey", "flowValue"), Collections.singleton(indexer));
      dataset.setMetadata(new MetadataEntry(dataset1, "datasetKey", "datasetValue"), Collections.singleton(indexer));
    });
    final String namespaceId = flow1.getValue(MetadataEntity.NAMESPACE);
    final Set<EntityTypeSimpleName> targetTypes = Collections.singleton(EntityTypeSimpleName.ALL);
    txnl.execute(() -> {
      List<MetadataEntry> searchResults = searchByDefaultIndex(dataset, namespaceId, "flowValue", targetTypes);
      Assert.assertTrue(searchResults.isEmpty());
      searchResults = searchByDefaultIndex(dataset, namespaceId, "flowKey:flow*", targetTypes);
      Assert.assertTrue(searchResults.isEmpty());
      searchResults = searchByDefaultIndex(dataset, namespaceId, "datasetValue", targetTypes);
      Assert.assertTrue(searchResults.isEmpty());
      searchResults = searchByDefaultIndex(dataset, namespaceId, "datasetKey:dataset*", targetTypes);
      Assert.assertTrue(searchResults.isEmpty());
    });
    final AtomicReference<byte[]> startRowKeyForNextBatch = new AtomicReference<>();
    txnl.execute(() -> {
      // Re-build indexes. Now the default indexer should be used
      startRowKeyForNextBatch.set(dataset.rebuildIndexes(null, 1));
      Assert.assertNotNull(startRowKeyForNextBatch.get());
    });
    txnl.execute(() -> {
      List<MetadataEntry> flowSearchResults = searchByDefaultIndex(dataset, namespaceId, "flowValue", targetTypes);
      List<MetadataEntry> dsSearchResults = searchByDefaultIndex(dataset, namespaceId, "datasetValue", targetTypes);
      if (!flowSearchResults.isEmpty()) {
        Assert.assertEquals(1, flowSearchResults.size());
        flowSearchResults = searchByDefaultIndex(dataset, namespaceId, "flowKey:flow*", targetTypes);
        Assert.assertEquals(1, flowSearchResults.size());
        Assert.assertTrue(dsSearchResults.isEmpty());
        dsSearchResults = searchByDefaultIndex(dataset, namespaceId, "datasetKey:dataset*", targetTypes);
        Assert.assertTrue(dsSearchResults.isEmpty());
      } else {
        flowSearchResults = searchByDefaultIndex(dataset, namespaceId, "flowKey:flow*", targetTypes);
        Assert.assertTrue(flowSearchResults.isEmpty());
        Assert.assertEquals(1, dsSearchResults.size());
        dsSearchResults = searchByDefaultIndex(dataset, namespaceId, "datasetKey:dataset*", targetTypes);
        Assert.assertEquals(1, dsSearchResults.size());
      }
    });
    txnl.execute(() -> {
      startRowKeyForNextBatch.set(dataset.rebuildIndexes(startRowKeyForNextBatch.get(), 1));
      Assert.assertNull(startRowKeyForNextBatch.get());
    });
    txnl.execute(() -> {
      List<MetadataEntry> searchResults = searchByDefaultIndex(dataset, namespaceId, "flowValue", targetTypes);
      Assert.assertEquals(1, searchResults.size());
      searchResults = searchByDefaultIndex(dataset, namespaceId, "flowKey:flow*", targetTypes);
      Assert.assertEquals(1, searchResults.size());
      searchResults = searchByDefaultIndex(dataset, namespaceId, "datasetValue", targetTypes);
      Assert.assertEquals(1, searchResults.size());
      searchResults = searchByDefaultIndex(dataset, namespaceId, "datasetKey:dataset*", targetTypes);
      Assert.assertEquals(1, searchResults.size());
    });
  }

  @Test
  public void testIndexDeletion() throws Exception {
    final MetadataDataset dataset =
      getDataset(DatasetFrameworkTestUtil.NAMESPACE_ID.dataset("testIndexRebuilding"));
    TransactionExecutor txnl = dsFrameworkUtil.newInMemoryTransactionExecutor((TransactionAware) dataset);
    txnl.execute(() -> {
      dataset.setProperty(flow1, "flowKey", "flowValue");
      dataset.setProperty(dataset1, "datasetKey", "datasetValue");
    });
    final String namespaceId = flow1.getValue(MetadataEntity.NAMESPACE);
    final Set<EntityTypeSimpleName> targetTypes = Collections.singleton(EntityTypeSimpleName.ALL);
    final MetadataEntry expectedFlowEntry = new MetadataEntry(flow1, "flowKey", "flowValue");
    final MetadataEntry expectedDatasetEntry = new MetadataEntry(dataset1, "datasetKey", "datasetValue");
    txnl.execute(() -> {
      List<MetadataEntry> searchResults = searchByDefaultIndex(dataset, namespaceId, "flowValue", targetTypes);
      Assert.assertEquals(ImmutableList.of(expectedFlowEntry), searchResults);
      searchResults = searchByDefaultIndex(dataset, namespaceId, "flowKey:flow*", targetTypes);
      Assert.assertEquals(ImmutableList.of(expectedFlowEntry), searchResults);
      searchResults = searchByDefaultIndex(dataset, namespaceId, "datasetValue", targetTypes);
      Assert.assertEquals(ImmutableList.of(expectedDatasetEntry), searchResults);
      searchResults = searchByDefaultIndex(dataset, namespaceId, "datasetKey:dataset*", targetTypes);
      Assert.assertEquals(ImmutableList.of(expectedDatasetEntry), searchResults);
    });
    // delete indexes
    // 4 indexes should have been deleted - flowValue, flowKey:flowValue, datasetValue, datasetKey:datasetValue
    for (int i = 0; i < 4; i++) {
      txnl.execute(() -> Assert.assertEquals(1, dataset.deleteAllIndexes(1)));
    }
    txnl.execute(() -> Assert.assertEquals(0, dataset.deleteAllIndexes(1)));
  }

  @Test
  public void testMultipleIndexes() throws Exception {
    final MetadataDataset dataset =
      getDataset(DatasetFrameworkTestUtil.NAMESPACE_ID.dataset("testMultipleIndexes"), MetadataScope.SYSTEM);
    TransactionExecutor txnl = dsFrameworkUtil.newInMemoryTransactionExecutor((TransactionAware) dataset);
    final String value = "value";
    final String body = "body";
    final String schema = Schema.recordOf("schema", Schema.Field.of(body, Schema.of(Schema.Type.BYTES))).toString();
    final String name = "dataset1";
    final long creationTime = System.currentTimeMillis();
    txnl.execute(() -> {
      dataset.setProperty(flow1, "key", value);
      dataset.setProperty(flow1, AbstractSystemMetadataWriter.SCHEMA_KEY, schema);
      dataset.setProperty(dataset1, AbstractSystemMetadataWriter.ENTITY_NAME_KEY, name);
      dataset.setProperty(dataset1, AbstractSystemMetadataWriter.CREATION_TIME_KEY, String.valueOf(creationTime));
    });
    final String namespaceId = flow1.getValue(MetadataEntity.NAMESPACE);
    txnl.execute(() -> {
      // entry with no special indexes
      assertSingleIndex(dataset, MetadataDataset.DEFAULT_INDEX_COLUMN, namespaceId, value);
      assertNoIndexes(dataset, MetadataDataset.ENTITY_NAME_INDEX_COLUMN, namespaceId, value);
      assertNoIndexes(dataset, MetadataDataset.INVERTED_ENTITY_NAME_INDEX_COLUMN, namespaceId, value);
      assertNoIndexes(dataset, MetadataDataset.CREATION_TIME_INDEX_COLUMN, namespaceId, value);
      assertNoIndexes(dataset, MetadataDataset.INVERTED_CREATION_TIME_INDEX_COLUMN, namespaceId, value);
      // entry with a schema
      assertSingleIndex(dataset, MetadataDataset.DEFAULT_INDEX_COLUMN, namespaceId, body);
      assertNoIndexes(dataset, MetadataDataset.ENTITY_NAME_INDEX_COLUMN, namespaceId, body);
      assertNoIndexes(dataset, MetadataDataset.INVERTED_ENTITY_NAME_INDEX_COLUMN, namespaceId, body);
      assertNoIndexes(dataset, MetadataDataset.CREATION_TIME_INDEX_COLUMN, namespaceId, body);
      assertNoIndexes(dataset, MetadataDataset.INVERTED_CREATION_TIME_INDEX_COLUMN, namespaceId, body);
      // entry with entity name
      assertSingleIndex(dataset, MetadataDataset.DEFAULT_INDEX_COLUMN, namespaceId, name);
      assertSingleIndex(dataset, MetadataDataset.ENTITY_NAME_INDEX_COLUMN, namespaceId, name);
      assertNoIndexes(dataset, MetadataDataset.INVERTED_ENTITY_NAME_INDEX_COLUMN, namespaceId, name);
      Indexer indexer = new InvertedValueIndexer();
      String index = Iterables.getOnlyElement(indexer.getIndexes(new MetadataEntry(dataset1, "key", name)));
      assertSingleIndex(dataset, MetadataDataset.INVERTED_ENTITY_NAME_INDEX_COLUMN, namespaceId, index.toLowerCase());
      assertNoIndexes(dataset, MetadataDataset.CREATION_TIME_INDEX_COLUMN, namespaceId, name);
      assertNoIndexes(dataset, MetadataDataset.INVERTED_CREATION_TIME_INDEX_COLUMN, namespaceId, name);
      // entry with creation time
      String time = String.valueOf(creationTime);
      assertSingleIndex(dataset, MetadataDataset.DEFAULT_INDEX_COLUMN, namespaceId, time);
      assertNoIndexes(dataset, MetadataDataset.ENTITY_NAME_INDEX_COLUMN, namespaceId, time);
      assertNoIndexes(dataset, MetadataDataset.INVERTED_ENTITY_NAME_INDEX_COLUMN, namespaceId, time);
      assertSingleIndex(dataset, MetadataDataset.CREATION_TIME_INDEX_COLUMN, namespaceId, time);
      assertNoIndexes(dataset, MetadataDataset.INVERTED_CREATION_TIME_INDEX_COLUMN, namespaceId, time);
      assertSingleIndex(dataset, MetadataDataset.INVERTED_CREATION_TIME_INDEX_COLUMN, namespaceId,
                        String.valueOf(Long.MAX_VALUE - creationTime));
    });
  }

  @Test
  public void testPagination() throws Exception {
    final MetadataDataset dataset =
      getDataset(DatasetFrameworkTestUtil.NAMESPACE_ID.dataset("testPagination"), MetadataScope.SYSTEM);
    TransactionExecutor txnl = dsFrameworkUtil.newInMemoryTransactionExecutor((TransactionAware) dataset);
    final String flowName = "name11";
    final String dsName = "name21 name22";
    final String appName = "name31 name32 name33";
    txnl.execute(() -> {
      dataset.setProperty(flow1, AbstractSystemMetadataWriter.ENTITY_NAME_KEY, flowName);
      dataset.setProperty(dataset1, AbstractSystemMetadataWriter.ENTITY_NAME_KEY, dsName);
      dataset.setProperty(app1, AbstractSystemMetadataWriter.ENTITY_NAME_KEY, appName);
    });
    final String namespaceId = flow1.getValue(MetadataEntity.NAMESPACE);
    final EnumSet<EntityTypeSimpleName> targets = EnumSet.allOf(EntityTypeSimpleName.class);
    final MetadataEntry flowEntry = new MetadataEntry(flow1, AbstractSystemMetadataWriter.ENTITY_NAME_KEY, flowName);
    final MetadataEntry dsEntry = new MetadataEntry(dataset1, AbstractSystemMetadataWriter.ENTITY_NAME_KEY, dsName);
    final MetadataEntry appEntry = new MetadataEntry(app1, AbstractSystemMetadataWriter.ENTITY_NAME_KEY, appName);
    txnl.execute(() -> {
      // since no sort is to be performed by the dataset, we return all (ignore limit and offset)
      SearchResults searchResults = dataset.search(namespaceId, "name*", targets, SortInfo.DEFAULT, 0, 3, 1, null,
                                                   false, EnumSet.allOf(EntityScope.class));
      Assert.assertEquals(
        // since default indexer is used:
        // 1 index for flow: 'name11'
        // 3 indexes for dataset: 'name21', 'name21 name22', 'name22'
        // 4 indexes for app: 'name31', 'name31 name32 name33', 'name32', 'name33'
        ImmutableList.of(flowEntry, dsEntry, dsEntry, dsEntry, appEntry, appEntry, appEntry, appEntry),
        searchResults.getResults()
      );
      // ascending sort by name. offset and limit should be respected.
      SortInfo nameAsc = new SortInfo(AbstractSystemMetadataWriter.ENTITY_NAME_KEY, SortInfo.SortOrder.ASC);
      // first 2 in ascending order
      searchResults = dataset.search(namespaceId, "*", targets, nameAsc, 0, 2, 0, null, false,
                                     EnumSet.allOf(EntityScope.class));
      Assert.assertEquals(ImmutableList.of(flowEntry, dsEntry), searchResults.getResults());
      // return 2 with offset 1 in ascending order
      searchResults = dataset.search(namespaceId, "*", targets, nameAsc, 1, 2, 0, null, false,
                                     EnumSet.allOf(EntityScope.class));
      Assert.assertEquals(ImmutableList.of(flowEntry, dsEntry, appEntry), searchResults.getResults());
      // descending sort by name. offset and filter should be respected.
      SortInfo nameDesc = new SortInfo(AbstractSystemMetadataWriter.ENTITY_NAME_KEY, SortInfo.SortOrder.DESC);
      // first 2 in descending order
      searchResults = dataset.search(namespaceId, "*", targets, nameDesc, 0, 2, 0, null, false,
                                     EnumSet.allOf(EntityScope.class));
      Assert.assertEquals(ImmutableList.of(appEntry, dsEntry), searchResults.getResults());
      // last 1 in descending order
      searchResults = dataset.search(namespaceId, "*", targets, nameDesc, 2, 1, 0, null, false,
                                     EnumSet.allOf(EntityScope.class));
      Assert.assertEquals(ImmutableList.of(appEntry, dsEntry, flowEntry), searchResults.getResults());
      searchResults = dataset.search(namespaceId, "*", targets, nameAsc, 2, 0, 0, null, false,
                                     EnumSet.allOf(EntityScope.class));
      Assert.assertEquals(ImmutableList.of(flowEntry, dsEntry), searchResults.getResults());
      searchResults = dataset.search(namespaceId, "*", targets, nameDesc, 1, 0, 0, null, false,
                                     EnumSet.allOf(EntityScope.class));
      Assert.assertEquals(ImmutableList.of(appEntry), searchResults.getResults());
      searchResults = dataset.search(namespaceId, "*", targets, nameAsc, 4, 0, 0, null, false,
                                     EnumSet.allOf(EntityScope.class));
      Assert.assertEquals(ImmutableList.of(flowEntry, dsEntry, appEntry), searchResults.getResults());
      searchResults = dataset.search(namespaceId, "*", targets, nameDesc, 100, 0, 0, null, false,
                                     EnumSet.allOf(EntityScope.class));
      Assert.assertEquals(ImmutableList.of(appEntry, dsEntry, flowEntry), searchResults.getResults());

      // test cursors
      searchResults = dataset.search(namespaceId, "*", targets, nameAsc, 0, 1, 3, null, false,
                                     EnumSet.allOf(EntityScope.class));
      Assert.assertEquals(ImmutableList.of(flowEntry, dsEntry, appEntry), searchResults.getResults());
      Assert.assertEquals(ImmutableList.of(dsName, appName), searchResults.getCursors());

      searchResults = dataset.search(namespaceId, "*", targets, nameAsc, 0, 1, 3, searchResults.getCursors().get(0),
                                     false,
                                     EnumSet.allOf(EntityScope.class));
      Assert.assertEquals(ImmutableList.of(dsEntry, appEntry), searchResults.getResults());
      Assert.assertEquals(ImmutableList.of(appName), searchResults.getCursors());

      searchResults = dataset.search(namespaceId, "*", targets, nameAsc, 0, 1, 3, searchResults.getCursors().get(0),
                                     false,
                                     EnumSet.allOf(EntityScope.class));
      Assert.assertEquals(ImmutableList.of(appEntry), searchResults.getResults());
      Assert.assertEquals(ImmutableList.of(), searchResults.getCursors());

      searchResults = dataset.search(namespaceId, "*", targets, nameAsc, 0, 2, 3, null, false,
                                     EnumSet.allOf(EntityScope.class));
      Assert.assertEquals(ImmutableList.of(flowEntry, dsEntry, appEntry), searchResults.getResults());
      Assert.assertEquals(ImmutableList.of(appName), searchResults.getCursors());

      searchResults = dataset.search(namespaceId, "*", targets, nameAsc, 3, 1, 2, null, false,
                                     EnumSet.allOf(EntityScope.class));
      Assert.assertEquals(ImmutableList.of(flowEntry, dsEntry, appEntry), searchResults.getResults());
      Assert.assertEquals(ImmutableList.of(), searchResults.getCursors());
    });
  }

  private void assertSingleIndex(final MetadataDataset dataset, final String indexColumn, final String namespaceId,
                                 final String value) throws InterruptedException, TransactionFailureException {
    final String searchQuery = namespaceId + MetadataDataset.KEYVALUE_SEPARATOR + value;
    try (Scanner scan = dataset.searchByIndex(indexColumn, searchQuery)) {
      Assert.assertNotNull(scan.next());
      Assert.assertNull(scan.next());
    }
  }

  private void assertNoIndexes(final MetadataDataset dataset, String indexColumn, String namespaceId, String value) {
    String searchQuery = namespaceId + MetadataDataset.KEYVALUE_SEPARATOR + value;
    try (Scanner scan = dataset.searchByIndex(indexColumn, searchQuery)) {
      Assert.assertNull(scan.next());
    }
  }

  private void doTestHistory(final MetadataDataset dataset, final MetadataEntity targetId, final String prefix)
    throws Exception {
    TransactionExecutor txnl = dsFrameworkUtil.newInMemoryTransactionExecutor((TransactionAware) dataset);

    // Metadata change history keyed by time in millis the change was made
    final Map<Long, Metadata> expected = new HashMap<>();
    // No history for targetId at the beginning
    txnl.execute(() -> {
      Metadata completeRecord = new Metadata(targetId);
      expected.put(System.currentTimeMillis(), completeRecord);
      // Get history for targetId, should be empty
      Assert.assertEquals(ImmutableSet.of(completeRecord),
                          dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), System.currentTimeMillis()));
      // Also, the metadata itself should be equal to the last recorded snapshot
      Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                 System.currentTimeMillis())),
                          new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    });

    // Since the key to expected map is time in millis, sleep for a millisecond to make sure the key is distinct
    TimeUnit.MILLISECONDS.sleep(1);

    // Add first record
    final Metadata completeRecord =
      new Metadata(targetId, toProps(prefix, "k1", "v1"), toTags(prefix, "t1", "t2"));
    txnl.execute(() -> addMetadataHistory(dataset, completeRecord));
    txnl.execute(() -> {
      long time = System.currentTimeMillis();
      expected.put(time, completeRecord);
      // Since this is the first record, history should be the same as what was added.
      Assert.assertEquals(ImmutableSet.of(completeRecord),
                          dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
      // Also, the metadata itself should be equal to the last recorded snapshot
      Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                 System.currentTimeMillis())),
                          new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    });
    TimeUnit.MILLISECONDS.sleep(1);

    txnl.execute(() -> {
      // Add a new property and a tag
      dataset.setProperty(targetId, prefix + "k2", "v2");
      dataset.addTags(targetId, prefix + "t3");
    });
    // Save the complete metadata record at this point
    txnl.execute(() -> {
      Metadata completeRecord1 = new Metadata(targetId, toProps(prefix, "k1", "v1", "k2", "v2"),
                                              toTags(prefix, "t1", "t2", "t3"));
      long time = System.currentTimeMillis();
      expected.put(time, completeRecord1);
      // Assert the history record with the change
      Assert.assertEquals(ImmutableSet.of(completeRecord1),
                          dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
      // Also, the metadata itself should be equal to the last recorded snapshot
      Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                 System.currentTimeMillis())),
                          new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    });
    TimeUnit.MILLISECONDS.sleep(1);

    txnl.execute(() -> {
      // Add another property and a tag
      dataset.setProperty(targetId, prefix + "k3", "v3");
      dataset.addTags(targetId, prefix + "t4");
    });
    txnl.execute(() -> {
      // Save the complete metadata record at this point
      Metadata completeRecord12 = new Metadata(targetId, toProps(prefix, "k1", "v1", "k2", "v2", "k3", "v3"),
                                               toTags(prefix, "t1", "t2", "t3", "t4"));
      long time = System.currentTimeMillis();
      expected.put(time, completeRecord12);
      // Assert the history record with the change
      Assert.assertEquals(ImmutableSet.of(completeRecord12),
                          dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
      // Also, the metadata itself should be equal to the last recorded snapshot
      Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                 System.currentTimeMillis())),
                          new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    });
    TimeUnit.MILLISECONDS.sleep(1);

    txnl.execute(() -> {
      // Add the same property and tag as second time
      dataset.setProperty(targetId, prefix + "k2", "v2");
      dataset.addTags(targetId, prefix + "t3");
    });
    txnl.execute(() -> {
      // Save the complete metadata record at this point
      Metadata completeRecord13 = new Metadata(targetId, toProps(prefix, "k1", "v1", "k2", "v2", "k3", "v3"),
                                               toTags(prefix, "t1", "t2", "t3", "t4"));
      long time = System.currentTimeMillis();
      expected.put(time, completeRecord13);
      // Assert the history record with the change
      Assert.assertEquals(ImmutableSet.of(completeRecord13),
                          dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
      // Also, the metadata itself should be equal to the last recorded snapshot
      Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                 System.currentTimeMillis())),
                          new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    });
    TimeUnit.MILLISECONDS.sleep(1);

    txnl.execute(() -> {
      // Remove a property and two tags
      dataset.removeProperties(targetId, prefix + "k2");
      dataset.removeTags(targetId, prefix + "t4");
      dataset.removeTags(targetId, prefix + "t2");
    });
    txnl.execute(() -> {
      // Save the complete metadata record at this point
      Metadata completeRecord14 = new Metadata(targetId, toProps(prefix, "k1", "v1", "k3", "v3"),
                                               toTags(prefix, "t1", "t3"));
      long time = System.currentTimeMillis();
      expected.put(time, completeRecord14);
      // Assert the history record with the change
      Assert.assertEquals(ImmutableSet.of(completeRecord14),
                          dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
      // Also, the metadata itself should be equal to the last recorded snapshot
      Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                 System.currentTimeMillis())),
                          new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    });
    TimeUnit.MILLISECONDS.sleep(1);

    txnl.execute(() -> {
      // Remove all properties and all tags
      dataset.removeProperties(targetId);
      dataset.removeTags(targetId);
    });
    txnl.execute(() -> {
      // Save the complete metadata record at this point
      Metadata completeRecord15 = new Metadata(targetId);
      long time = System.currentTimeMillis();
      expected.put(time, completeRecord15);
      // Assert the history record with the change
      Assert.assertEquals(ImmutableSet.of(completeRecord15),
                          dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
      // Also, the metadata itself should be equal to the last recorded snapshot
      Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                 System.currentTimeMillis())),
                          new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    });
    TimeUnit.MILLISECONDS.sleep(1);

    // Add one more property and a tag
    txnl.execute(() -> {
      dataset.setProperty(targetId, prefix + "k2", "v2");
      dataset.addTags(targetId, prefix + "t2");
    });
    final Metadata lastCompleteRecord = new Metadata(targetId, toProps(prefix, "k2", "v2"),
                                                     toTags(prefix, "t2"));
    txnl.execute(() -> {
      // Save the complete metadata record at this point
      long time = System.currentTimeMillis();
      expected.put(time, lastCompleteRecord);
      // Assert the history record with the change
      Assert.assertEquals(ImmutableSet.of(lastCompleteRecord),
                          dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
      // Also, the metadata itself should be equal to the last recorded snapshot
      Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                 System.currentTimeMillis())),
                          new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    });
    TimeUnit.MILLISECONDS.sleep(1);

    // Now assert all history
    txnl.execute(() -> {
      for (Map.Entry<Long, Metadata> entry : expected.entrySet()) {
        Assert.assertEquals(entry.getValue(),
                            getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), entry.getKey())));
      }
      // Asserting for current time should give the latest record
      Assert.assertEquals(ImmutableSet.of(lastCompleteRecord),
                          dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), System.currentTimeMillis()));
      // Also, the metadata itself should be equal to the last recorded snapshot
      Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                 System.currentTimeMillis())),
                          new Metadata(targetId, dataset.getProperties(targetId), dataset.getTags(targetId)));
    });
  }

  private void addMetadataHistory(MetadataDataset dataset, Metadata record) {
    for (Map.Entry<String, String> entry : record.getProperties().entrySet()) {
      dataset.setProperty(record.getMetadataEntity(), entry.getKey(), entry.getValue());
    }
    //noinspection ToArrayCallWithZeroLengthArrayArgument
    dataset.addTags(record.getMetadataEntity(), record.getTags().toArray(new String[0]));
  }

  private Map<String, String> toProps(String prefix, String k1, String v1) {
    return ImmutableMap.of(prefix + k1, v1);
  }

  private Map<String, String> toProps(String prefix, String k1, String v1, String k2, String v2) {
    return ImmutableMap.of(prefix + k1, v1, prefix + k2, v2);
  }

  private Map<String, String>
  toProps(String prefix, String k1, String v1, String k2, String v2, String k3, String v3) {
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

  // should be called inside a transaction. This method does not start a transaction on its own, so that you can
  // have multiple invocations in the same transaction.
  private List<MetadataEntry> searchByDefaultIndex(String namespaceId, String searchQuery,
                                                   Set<EntityTypeSimpleName> types) throws BadRequestException {
    return searchByDefaultIndex(dataset, namespaceId, searchQuery, types);
  }

  // should be called inside a transaction. This method does not start a transaction on its own, so that you can
  // have multiple invocations in the same transaction.
  private List<MetadataEntry> searchByDefaultIndex(MetadataDataset dataset, String namespaceId, String searchQuery,
                                                   Set<EntityTypeSimpleName> types) throws BadRequestException {
    return dataset.search(namespaceId, searchQuery, types,
                          SortInfo.DEFAULT, 0, Integer.MAX_VALUE, 1, null, false,
                          EnumSet.allOf(EntityScope.class)).getResults();
  }

  private static MetadataDataset getDataset(DatasetId instance) throws Exception {
    return getDataset(instance, MetadataScope.USER);
  }

  private static MetadataDataset getDataset(DatasetId instance, MetadataScope scope) throws Exception {
    return DatasetsUtil.getOrCreateDataset(dsFrameworkUtil.getFramework(), instance,
                                           MetadataDataset.class.getName(),
                                           DatasetProperties.builder()
                                             .add(MetadataDatasetDefinition.SCOPE_KEY, scope.name())
                                             .build(),
                                           null);
  }

  private static final class ReversingIndexer implements Indexer {

    @Override
    public Set<String> getIndexes(MetadataEntry entry) {
      return ImmutableSet.of(reverse(entry.getKey()), reverse(entry.getValue()));
    }

    @Override
    public SortInfo.SortOrder getSortOrder() {
      return SortInfo.SortOrder.WEIGHTED;
    }

    private String reverse(String toReverse) {
      return new StringBuilder(toReverse).reverse().toString();
    }
  }
}
