/*
 * Copyright 2015-2019 Cask Data, Inc.
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
package io.cdap.cdap.data2.metadata.dataset;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.inject.Key;
import com.google.inject.name.Names;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetContext;
import io.cdap.cdap.api.dataset.DatasetDefinition;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.DatasetSpecification;
import io.cdap.cdap.api.dataset.lib.IndexedTableDefinition;
import io.cdap.cdap.api.dataset.table.Scanner;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.data2.dataset2.DatasetFrameworkTestUtil;
import io.cdap.cdap.data2.metadata.indexer.Indexer;
import io.cdap.cdap.data2.metadata.indexer.InvertedValueIndexer;
import io.cdap.cdap.proto.EntityScope;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.spi.metadata.MetadataConstants;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionFailureException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Test class for {@link MetadataDataset} class.
 */
public class MetadataDatasetTest {

  @ClassRule
  public static DatasetFrameworkTestUtil dsFrameworkUtil = new DatasetFrameworkTestUtil();

  private MetadataDatasetDefinition definition;
  private DatasetSpecification spec;
  private DatasetAdmin admin;
  private MetadataDataset dataset;
  private TransactionExecutor txnl;

  private static final Set<String> ALL_TYPES = Collections.emptySet();

  private final MetadataEntity app1 = new ApplicationId("ns1", "app1").toMetadataEntity();
  private final MetadataEntity appNs2 = new ApplicationId("ns2", "app1").toMetadataEntity();
  // Have to use Id.Program for comparison here because the MetadataDataset APIs return Id.Program.
  private final MetadataEntity program1 = new ProgramId("ns1", "app1", ProgramType.WORKER, "wk1").toMetadataEntity();
  private final MetadataEntity dataset1 = new DatasetId("ns1", "ds1").toMetadataEntity();
  private final MetadataEntity dataset2 = new DatasetId("ns1", "ds2").toMetadataEntity();
  private final MetadataEntity artifact1 = new ArtifactId("ns1", "a1", "1.0.0").toMetadataEntity();
  private final MetadataEntity fileEntity = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns1")
    .append(MetadataEntity.DATASET, "ds1").appendAsType("file", "f1").build();
  private final MetadataEntity partitionFileEntity = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns1")
    .append(MetadataEntity.DATASET, "ds1").append("partition", "p1")
    .appendAsType("file", "f1").build();
  private final MetadataEntity jarEntity = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns1")
    .appendAsType("jar", "jar1").build();

  @SuppressWarnings("unchecked")
  @Before
  public void before() throws Exception {
    DatasetContext datasetContext = DatasetContext.from(NamespaceId.SYSTEM.getNamespace());
    if (admin == null) {
      DatasetDefinition tableDefinition = dsFrameworkUtil.getInjector()
        .getInstance(Key.get(DatasetDefinition.class, Names.named(Constants.Dataset.TABLE_TYPE)));
      definition = new MetadataDatasetDefinition(
        MetadataDataset.TYPE, new IndexedTableDefinition("indexedTable", tableDefinition));
      String scope = MetadataScope.SYSTEM.name();
      spec = definition.configure(
        "testMetadata", DatasetProperties.builder().add(MetadataDatasetDefinition.SCOPE_KEY, scope).build());
      admin = definition.getAdmin(datasetContext, spec, null);
    }
    if (!admin.exists()) {
      admin.create();
    }
    dataset = definition.getDataset(datasetContext, spec, Collections.emptyMap(), null);
    txnl = dsFrameworkUtil.newInMemoryTransactionExecutor((TransactionAware) dataset);
  }

  @After
  public void after() throws Exception {
    dataset.close();
    dataset = null;
    admin.drop();
  }

  @Test
  public void testProperties() throws Exception {
    txnl.execute(() -> {
      Assert.assertEquals(0, dataset.getProperties(app1).size());
      Assert.assertEquals(0, dataset.getProperties(program1).size());
      Assert.assertEquals(0, dataset.getProperties(dataset1).size());
      Assert.assertEquals(0, dataset.getProperties(dataset2).size());
      Assert.assertEquals(0, dataset.getProperties(artifact1).size());
      Assert.assertEquals(0, dataset.getProperties(fileEntity).size());
      Assert.assertEquals(0, dataset.getProperties(partitionFileEntity).size());
      Assert.assertEquals(0, dataset.getProperties(jarEntity).size());
    });
    // Set some properties
    txnl.execute(() -> {
      dataset.addProperty(app1, "akey1", "avalue1");
      MetadataDataset.Change metadataChange = dataset.addProperties(program1, Collections.emptyMap());
      Assert.assertEquals(metadataChange.getExisting(), new MetadataDataset.Record(program1, Collections.emptyMap(),
                                                                                   Collections.emptySet()));
      Assert.assertEquals(metadataChange.getLatest(), new MetadataDataset.Record(program1, Collections.emptyMap(),
                                                                                 Collections.emptySet()));
      metadataChange = dataset.addProperty(program1, "fkey1", "fvalue1");
      // assert the metadata change which happens on setting property for the first time
      Assert.assertEquals(new MetadataDataset.Record(program1), metadataChange.getExisting());
      Assert.assertEquals(new MetadataDataset.Record(program1,
                                                     ImmutableMap.of("fkey1", "fvalue1"), Collections.emptySet()),
                          metadataChange.getLatest());
      metadataChange = dataset.addProperty(program1, "fK", "fV");
      // assert the metadata change which happens when setting property with existing property
      Assert.assertEquals(new MetadataDataset.Record(program1,
                                                     ImmutableMap.of("fkey1", "fvalue1"), Collections.emptySet()),
                          metadataChange.getExisting());
      Assert.assertEquals(new MetadataDataset.Record(program1,
                                                     ImmutableMap.of("fkey1", "fvalue1", "fK", "fV"),
                                                     Collections.emptySet()),
                          metadataChange.getLatest());
      dataset.addProperty(dataset1, "dkey1", "dvalue1");
      dataset.addProperty(dataset2, "skey1", "svalue1");
      dataset.addProperty(dataset2, "skey2", "svalue2");
      dataset.addProperty(artifact1, "rkey1", "rvalue1");
      dataset.addProperty(artifact1, "rkey2", "rvalue2");
      dataset.addProperty(fileEntity, "fkey2", "fvalue2");
      dataset.addProperty(partitionFileEntity, "pfkey2", "pfvalue2");
      dataset.addProperty(jarEntity, "jkey2", "jvalue2");
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
      MetadataEntry result = dataset.getProperty(program1, "fkey1");
      MetadataEntry expected = new MetadataEntry(program1, "fkey1", "fvalue1");
      Assert.assertEquals(expected, result);
      Assert.assertEquals(ImmutableMap.of("fkey1", "fvalue1", "fK", "fV"), dataset.getProperties(program1));
    });
    txnl.execute(() -> dataset.removeProperties(program1, "fkey1"));
    txnl.execute(() -> {
      Map<String, String> properties = dataset.getProperties(program1);
      Assert.assertEquals(1, properties.size());
      Assert.assertEquals("fV", properties.get("fK"));
    });
    txnl.execute(() -> dataset.removeProperties(program1));
    txnl.execute(() -> {
      Assert.assertEquals(0, dataset.getProperties(program1).size());
      Assert.assertEquals(0, dataset.getProperties(jarEntity).size());
      MetadataEntry expected = new MetadataEntry(dataset1, "dkey1", "dvalue1");
      Assert.assertEquals(expected, dataset.getProperty(dataset1, "dkey1"));
      Assert.assertEquals(ImmutableMap.of("skey1", "svalue1", "skey2", "svalue2"), dataset.getProperties(dataset2));
      Map<String, String> properties = dataset.getProperties(artifact1);
      Assert.assertEquals(ImmutableMap.of("rkey1", "rvalue1", "rkey2", "rvalue2"), properties);
      MetadataEntry result = dataset.getProperty(artifact1, "rkey2");
      expected = new MetadataEntry(artifact1, "rkey2", "rvalue2");
      Assert.assertEquals(expected, result);
      result = dataset.getProperty(fileEntity, "fkey2");
      expected = new MetadataEntry(fileEntity, "fkey2", "fvalue2");
      Assert.assertEquals(expected, result);
      result = dataset.getProperty(partitionFileEntity, "pfkey2");
      expected = new MetadataEntry(partitionFileEntity, "pfkey2", "pfvalue2");
      Assert.assertEquals(expected, result);
    });
    // reset a property
    txnl.execute(() -> dataset.addProperty(dataset2, "skey1", "sv1"));
    txnl.execute(() -> Assert.assertEquals(ImmutableMap.of("skey1", "sv1", "skey2", "svalue2"),
                                           dataset.getProperties(dataset2)));
    // cleanup
    txnl.execute(() -> {
      dataset.removeProperties(app1);
      dataset.removeProperties(program1);
      dataset.removeProperties(dataset1);
      dataset.removeProperties(dataset2);
      dataset.removeProperties(artifact1);
      dataset.removeProperties(fileEntity);
      dataset.removeProperties(partitionFileEntity);
    });
    txnl.execute(() -> {
      Assert.assertEquals(0, dataset.getProperties(app1).size());
      Assert.assertEquals(0, dataset.getProperties(program1).size());
      Assert.assertEquals(0, dataset.getProperties(dataset1).size());
      Assert.assertEquals(0, dataset.getProperties(dataset2).size());
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
      Assert.assertEquals(0, dataset.getTags(program1).size());
      Assert.assertEquals(0, dataset.getTags(dataset1).size());
      Assert.assertEquals(0, dataset.getTags(dataset2).size());
      Assert.assertEquals(0, dataset.getTags(artifact1).size());
      Assert.assertEquals(0, dataset.getTags(fileEntity).size());
      Assert.assertEquals(0, dataset.getTags(partitionFileEntity).size());
      Assert.assertEquals(0, dataset.getTags(jarEntity).size());
    });
    txnl.execute(() -> {
      dataset.addTags(app1, "tag1", "tag2", "tag3");
      MetadataDataset.Change metadataChange = dataset.addTags(program1, Collections.emptySet());
      Assert.assertEquals(metadataChange.getExisting(), new MetadataDataset.Record(program1, Collections.emptyMap(),
                                                                                   Collections.emptySet()));
      Assert.assertEquals(metadataChange.getLatest(), new MetadataDataset.Record(program1, Collections.emptyMap(),
                                                                                 Collections.emptySet()));
      metadataChange = dataset.addTags(program1, "tag1");
      // assert the metadata change which happens on setting tag for the first time
      Assert.assertEquals(new MetadataDataset.Record(program1), metadataChange.getExisting());
      Assert.assertEquals(new MetadataDataset.Record(program1, Collections.emptyMap(), ImmutableSet.of("tag1")),
                          metadataChange.getLatest());
      metadataChange = dataset.addTags(program1, "tag2");
      // assert the metadata change which happens on setting tag when a tag exists
      Assert.assertEquals(new MetadataDataset.Record(program1, Collections.emptyMap(), ImmutableSet.of("tag1")),
                          metadataChange.getExisting());
      Assert.assertEquals(new MetadataDataset.Record(program1, Collections.emptyMap(), ImmutableSet.of("tag1", "tag2")),
                          metadataChange.getLatest());
      dataset.addTags(dataset1, "tag3", "tag2");
      dataset.addTags(dataset2, "tag2");
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
    txnl.execute(() -> {
      Assert.assertEquals(3, dataset.getTags(app1).size());
      // adding an existing tags should not be added
      dataset.addTags(app1, "tag2");
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
      Set<String> tags = dataset.getTags(program1);
      Assert.assertEquals(2, tags.size());
      Assert.assertTrue(tags.containsAll(ImmutableSet.of("tag1", "tag2")));
      tags = dataset.getTags(dataset1);
      Assert.assertEquals(2, tags.size());
      Assert.assertTrue(tags.contains("tag3"));
      Assert.assertTrue(tags.contains("tag2"));
      tags = dataset.getTags(dataset2);
      Assert.assertEquals(1, tags.size());
      Assert.assertTrue(tags.contains("tag2"));
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
      dataset.removeTags(program1);
      dataset.removeTags(dataset1);
      dataset.removeTags(dataset2);
      dataset.removeTags(artifact1);
      dataset.removeTags(fileEntity);
      dataset.removeTags(partitionFileEntity);
      dataset.removeTags(jarEntity);
    });
    txnl.execute(() -> {
      Assert.assertEquals(0, dataset.getTags(app1).size());
      Assert.assertEquals(0, dataset.getTags(program1).size());
      Assert.assertEquals(0, dataset.getTags(dataset1).size());
      Assert.assertEquals(0, dataset.getTags(dataset2).size());
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
      Assert.assertEquals(0, dataset.getTags(program1).size());
      Assert.assertEquals(0, dataset.getTags(dataset1).size());
      Assert.assertEquals(0, dataset.getTags(dataset2).size());
      Assert.assertEquals(0, dataset.getTags(fileEntity).size());
      dataset.addTags(app1, "tag1", "tag2", "tag3");
      dataset.addTags(appNs2, "tag1", "tag2", "tag3_more");
      dataset.addTags(program1, "tag1");
      dataset.addTags(dataset1, "tag3", "tag2", "tag12-tag33");
      dataset.addTags(dataset2, "tag2, tag4");
      dataset.addTags(fileEntity, "tag2, tag5");
    });

    // Try to search on all tags
    txnl.execute(() -> {
      List<MetadataEntry> results =
        searchByDefaultIndex("ns1", "tags:*", ALL_TYPES);
      // results tags in ns1 are app1-tag1, tag2, tag3 + program1-tag1 + dataset1-tag3, tag2, tag12-tag33, tag12, tag33
      // + dataset2-tag2, tag4 + fileEntity-tag2, tag5
      Assert.assertEquals(13, results.size());

      // Try to search for tag1*
      results = searchByDefaultIndex("ns1", "tags:tag1*", ALL_TYPES);
      Assert.assertEquals(4, results.size());

      // Try to search for tag1 with spaces in search query and mixed case of tags keyword
      results = searchByDefaultIndex("ns1", "  tAGS  :  tag1  ", ALL_TYPES);
      Assert.assertEquals(2, results.size());

      // Try to search for tag5
      results = searchByDefaultIndex("ns1", "tags:tag5", ALL_TYPES);
      Assert.assertEquals(1, results.size());

      // Try to search for tag2
      results = searchByDefaultIndex("ns1", "tags:tag2", ALL_TYPES);
      Assert.assertEquals(4, results.size());

      // Try to search for tag4
      results = searchByDefaultIndex("ns1", "tags:tag4", ALL_TYPES);
      Assert.assertEquals(1, results.size());

      // Try to search for tag33
      results = searchByDefaultIndex("ns1", "tags:tag33", ALL_TYPES);
      Assert.assertEquals(1, results.size());

      // Try to search for a tag which has - in it
      results = searchByDefaultIndex("ns1", "tag12-tag33", ALL_TYPES);
      Assert.assertEquals(1, results.size());

      // Try to search for tag33 with spaces in query
      results = searchByDefaultIndex("ns1", "  tag33  ", ALL_TYPES);
      Assert.assertEquals(1, results.size());

      // Try to search for tag3
      results = searchByDefaultIndex("ns1", "tags:tag3*", ALL_TYPES);
      Assert.assertEquals(3, results.size());

      // try search in another namespace
      results = searchByDefaultIndex("ns2", "tags:tag1", ALL_TYPES);
      Assert.assertEquals(1, results.size());

      results = searchByDefaultIndex("ns2", "tag3", ALL_TYPES);
      Assert.assertEquals(1, results.size());

      results = searchByDefaultIndex("ns2", "tag*", ImmutableSet.of(MetadataEntity.APPLICATION));
      // 9 due to matches of type ns2:tag1, ns2:tags:tag1, and splitting of tag3_more
      Assert.assertEquals(9, results.size());

    });
    // cleanup
    txnl.execute(() -> {
      dataset.removeTags(app1);
      dataset.removeTags(program1);
      dataset.removeTags(dataset1);
      dataset.removeTags(dataset2);
    });
    // Search should be empty after deleting tags
    txnl.execute(() -> {
      List<MetadataEntry> results =
        searchByDefaultIndex("ns1", "tags:tag3*", ALL_TYPES);
      Assert.assertEquals(0, results.size());
      Assert.assertEquals(0, dataset.getTags(app1).size());
      Assert.assertEquals(0, dataset.getTags(program1).size());
      Assert.assertEquals(0, dataset.getTags(dataset1).size());
      Assert.assertEquals(0, dataset.getTags(dataset2).size());
    });
  }

  @Test
  public void testSearchOnTypes() throws Exception {
    MetadataEntity myField1 =
      MetadataEntity.builder(MetadataEntity.ofDataset(NamespaceId.DEFAULT.getEntityName(), "myDs"))
        .appendAsType("field", "myField1").build();
    MetadataEntity myField2 =
      MetadataEntity.builder(MetadataEntity.ofDataset(NamespaceId.DEFAULT.getEntityName(), "myDs"))
        .appendAsType("field", "myField2").build();
    final MetadataEntry myFieldEntry1 = new MetadataEntry(myField1, "testKey1", "testValue1");
    final MetadataEntry myFieldEntry2 = new MetadataEntry(myField2, "testKey2", "testValue2");
    txnl.execute(() -> {
      dataset.addProperty(myField1, "testKey1", "testValue1");
      dataset.addProperty(myField2, "testKey2", "testValue2");
    });

    // Search for it based on value
    txnl.execute(() -> {
      List<MetadataEntry> results =
        searchByDefaultIndex("default", "field:myField1", ALL_TYPES);
      Assert.assertEquals(ImmutableList.of(myFieldEntry1), results);

      // should return both fields
      results =
        searchByDefaultIndex("default", "field:myFie*", ALL_TYPES);
      Assert.assertEquals(ImmutableList.of(myFieldEntry1, myFieldEntry2), results);

      results =
        searchByDefaultIndex("default", "field*", ALL_TYPES);
      Assert.assertEquals(ImmutableList.of(myFieldEntry1, myFieldEntry2), results);
    });
  }

  @Test
  public void testSearchOnValue() throws Exception {
    // Add some metadata
    final MetadataEntry entry = new MetadataEntry(program1, "key1", "value1");
    final String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";
    final MetadataEntry multiWordEntry = new MetadataEntry(program1, "multiword", multiWordValue);

    txnl.execute(() -> {
      dataset.addProperty(program1, "key1", "value1");
      dataset.addProperty(program1, "key2", "value2");
      dataset.addProperty(program1, "multiword", multiWordValue);
    });

    // Search for it based on value
    txnl.execute(() -> {
      List<MetadataEntry> results =
        searchByDefaultIndex("ns1", "value1", ImmutableSet.of(MetadataEntity.PROGRAM));
      Assert.assertEquals(ImmutableList.of(entry), results);

      // Search for it based on a word in value with spaces in search query
      results = searchByDefaultIndex("ns1", "  aV1   ", ImmutableSet.of(MetadataEntity.PROGRAM));
      Assert.assertEquals(ImmutableList.of(multiWordEntry), results);

      // Search for it based split patterns to make sure nothing is matched
      results = searchByDefaultIndex("ns1", "-", ALL_TYPES);
      Assert.assertTrue(results.isEmpty());
      results = searchByDefaultIndex("ns1", ",", ALL_TYPES);
      Assert.assertTrue(results.isEmpty());
      results = searchByDefaultIndex("ns1", "_", ALL_TYPES);
      Assert.assertTrue(results.isEmpty());
      results = searchByDefaultIndex("ns1", ", ,", ALL_TYPES);
      Assert.assertTrue(results.isEmpty());
      results = searchByDefaultIndex("ns1", ", - ,", ALL_TYPES);
      Assert.assertTrue(results.isEmpty());

      // Search for it based on a word in value
      results = searchByDefaultIndex("ns1", "av5", ImmutableSet.of(MetadataEntity.PROGRAM));
      Assert.assertEquals(ImmutableList.of(multiWordEntry), results);

      // Case insensitive
      results = searchByDefaultIndex("ns1", "ValUe1", ImmutableSet.of(MetadataEntity.PROGRAM));
      Assert.assertEquals(ImmutableList.of(entry), results);

    });
    // Search based on value
    txnl.execute(() -> dataset.addProperty(program1, "key3", "value1"));
    txnl.execute(() -> {
      List<MetadataEntry> results =
        searchByDefaultIndex("ns1", "value1", ImmutableSet.of(MetadataEntity.PROGRAM));
      Assert.assertEquals(2, results.size());
      for (MetadataEntry result : results) {
        Assert.assertEquals("value1", result.getValue());
      }
    });

    // Search based on value prefix
    txnl.execute(() -> dataset.addProperty(dataset2, "key21", "value21"));
    txnl.execute(() -> {
      List<MetadataEntry> results =
        searchByDefaultIndex("ns1", "value2*", ALL_TYPES);
      Assert.assertEquals(2, results.size());
      for (MetadataEntry result : results) {
        Assert.assertTrue(result.getValue().startsWith("value2"));
      }
      // Search based on value prefix in the wrong namespace
      results = searchByDefaultIndex("ns12", "value2*", ALL_TYPES);
      Assert.assertTrue(results.isEmpty());
    });

  }

  @Test
  public void testSearchOnKeyValue() throws Exception {
    final MetadataEntry flowEntry1 = new MetadataEntry(program1, "key1", "value1");
    final MetadataEntry flowEntry2 = new MetadataEntry(program1, "key2", "value2");
    final String multiWordKey = "multiword";
    final String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";
    final MetadataEntry streamEntry1 = new MetadataEntry(dataset2, "Key1", "Value1");
    final MetadataEntry streamEntry2 = new MetadataEntry(dataset2, "sKey1", "sValue1");

    txnl.execute(() -> {
      // Add some properties to program1
      dataset.addProperty(program1, "key1", "value1");
      dataset.addProperty(program1, "key2", "value2");
      // add a multi word value
      dataset.addProperty(program1, multiWordKey, multiWordValue);
      dataset.addProperty(dataset2, "sKey1", "sValue1");
      dataset.addProperty(dataset2, "Key1", "Value1");
    });

    txnl.execute(() -> {
      // Search for it based on value
      List<MetadataEntry> results =
        searchByDefaultIndex("ns1", "key1" + MetadataConstants.KEYVALUE_SEPARATOR + "value1",
                             ImmutableSet.of(MetadataEntity.PROGRAM));
      Assert.assertEquals(ImmutableList.of(flowEntry1), results);

      // Search for it based on a word in value with spaces in search query
      results = searchByDefaultIndex("ns1", "  multiword" + MetadataConstants.KEYVALUE_SEPARATOR + "aV1   ",
                                     ImmutableSet.of(MetadataEntity.PROGRAM));

      MetadataEntry flowMultiWordEntry = new MetadataEntry(program1, multiWordKey, multiWordValue);
      Assert.assertEquals(ImmutableList.of(flowMultiWordEntry), results);

      // Search for it based on a word in value
      results =
        searchByDefaultIndex("ns1", multiWordKey + MetadataConstants.KEYVALUE_SEPARATOR + "aV5",
                             ImmutableSet.of(MetadataEntity.PROGRAM));
      Assert.assertEquals(ImmutableList.of(flowMultiWordEntry), results);
    });
    txnl.execute(() -> dataset.removeProperties(program1, multiWordKey));
    txnl.execute(() -> {
      List<MetadataEntry> results =
        searchByDefaultIndex("ns1", multiWordKey + MetadataConstants.KEYVALUE_SEPARATOR + "aV5",
                             ImmutableSet.of(MetadataEntity.PROGRAM));
      // search results should be empty after removing this key as the indexes are deleted
      Assert.assertTrue(results.isEmpty());

      // Test wrong ns
      List<MetadataEntry> results2 =
        searchByDefaultIndex("ns12", "key1" + MetadataConstants.KEYVALUE_SEPARATOR + "value1",
                             ImmutableSet.of(MetadataEntity.PROGRAM));
      Assert.assertTrue(results2.isEmpty());

      // Test multi word query
      results = searchByDefaultIndex("ns1", "  value1  av2 ", ALL_TYPES);
      Assert.assertEquals(Sets.newHashSet(flowEntry1, streamEntry1), Sets.newHashSet(results));

      results = searchByDefaultIndex("ns1", "  value1  sValue1 ", ALL_TYPES);
      Assert.assertEquals(Sets.newHashSet(flowEntry1, streamEntry1, streamEntry2), Sets.newHashSet(results));

      results = searchByDefaultIndex("ns1", "  valu*  sVal* ", ALL_TYPES);
      Assert.assertEquals(Sets.newHashSet(flowEntry1, flowEntry2, streamEntry1, streamEntry2),
                          Sets.newHashSet(results));

      // Using empty filter should also search for all target types
      results = searchByDefaultIndex("ns1", "  valu*  sVal* ", ImmutableSet.of());
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
      dataset.addProperty(program1, multiWordKey, multiWordValue);
      dataset.addProperty(sysArtifact, multiWordKey, multiWordValue);
      dataset.addProperty(ns2Artifact, multiWordKey, multiWordValue);
    });
    // perform the exact same multiword search in the 'ns1' namespace. It should return the system artifact along with
    // matched entities in the 'ns1' namespace
    final MetadataEntry flowMultiWordEntry = new MetadataEntry(program1, multiWordKey, multiWordValue);
    final MetadataEntry systemArtifactEntry = new MetadataEntry(sysArtifact, multiWordKey, multiWordValue);
    final MetadataEntry ns2ArtifactEntry = new MetadataEntry(ns2Artifact, multiWordKey, multiWordValue);

    txnl.execute(() -> {
      List<MetadataEntry> results = searchByDefaultIndex("ns1", "aV5", ALL_TYPES);
      Assert.assertEquals(Sets.newHashSet(flowMultiWordEntry, systemArtifactEntry), Sets.newHashSet(results));
      // search only programs - should only return flow
      results = searchByDefaultIndex("ns1", multiWordKey + MetadataConstants.KEYVALUE_SEPARATOR + "aV5",
                                     ImmutableSet.of(MetadataEntity.PROGRAM));
      Assert.assertEquals(ImmutableList.of(flowMultiWordEntry), results);
      // search only artifacts - should only return system artifact
      results = searchByDefaultIndex("ns1", multiWordKey + MetadataConstants.KEYVALUE_SEPARATOR + multiWordValue,
                                     ImmutableSet.of(MetadataEntity.ARTIFACT));
      // this query returns the system artifact 4 times, since the dataset returns a list with duplicates for scoring
      // purposes. Convert to a Set for comparison.
      Assert.assertEquals(Sets.newHashSet(systemArtifactEntry), Sets.newHashSet(results));
      // search all entities in namespace 'ns2' - should return the system artifact and the same artifact in ns2
      results = searchByDefaultIndex("ns2", multiWordKey + MetadataConstants.KEYVALUE_SEPARATOR + "aV4", ALL_TYPES);
      Assert.assertEquals(Sets.newHashSet(systemArtifactEntry, ns2ArtifactEntry), Sets.newHashSet(results));
      // search only programs in a namespace 'ns2'. Should return empty
      results = searchByDefaultIndex("ns2", "aV*", ImmutableSet.of(MetadataEntity.PROGRAM));
      Assert.assertTrue(results.isEmpty());
      // search all entities in namespace 'ns3'. Should return only the system artifact
      results = searchByDefaultIndex("ns3", "av*", ALL_TYPES);
      Assert.assertEquals(Sets.newHashSet(systemArtifactEntry), Sets.newHashSet(results));
      // search the system namespace for all entities. Should return only the system artifact
      results = searchByDefaultIndex(NamespaceId.SYSTEM.getEntityName(), "av*", ALL_TYPES);
      Assert.assertEquals(Sets.newHashSet(systemArtifactEntry), Sets.newHashSet(results));
    });
    // clean up
    txnl.execute(() -> {
      dataset.removeProperties(program1);
      dataset.removeProperties(sysArtifact);
    });
  }

  @Test
  public void testDetermineSearchFields() {
    String term = "a";
    String myns = "myns";
    Optional<NamespaceId> noNs = Optional.empty();
    Optional<NamespaceId> userNs = Optional.of(new NamespaceId(myns));
    Optional<NamespaceId> systemNs = Optional.of(NamespaceId.SYSTEM);
    Set<EntityScope> noScopes = Collections.emptySet();
    Set<EntityScope> userScope = Collections.singleton(EntityScope.USER);
    Set<EntityScope> systemScope = Collections.singleton(EntityScope.SYSTEM);
    Set<EntityScope> allScopes = EnumSet.allOf(EntityScope.class);
    MetadataDataset.SearchTerm a = MetadataDataset.SearchTerm.from(term);
    MetadataDataset.SearchTerm userA = MetadataDataset.SearchTerm.from(userNs.get(), term);
    MetadataDataset.SearchTerm systemA = MetadataDataset.SearchTerm.from(NamespaceId.SYSTEM, term);

    List<Triple<Optional<NamespaceId>, Set<EntityScope>, Set<MetadataDataset.SearchTerm>>> cases =
      ImmutableList.<Triple<Optional<NamespaceId>, Set<EntityScope>, Set<MetadataDataset.SearchTerm>>>builder()
        .add(Triple.of(noNs,     noScopes,    ImmutableSet.of()))
        .add(Triple.of(noNs,     userScope,   ImmutableSet.of(a))) // this should really be "any ns other than system"
        .add(Triple.of(noNs,     systemScope, ImmutableSet.of(systemA)))
        .add(Triple.of(noNs,     allScopes,   ImmutableSet.of(a)))
        .add(Triple.of(userNs,   noScopes,    ImmutableSet.of()))
        .add(Triple.of(userNs,   userScope,   ImmutableSet.of(userA)))
        .add(Triple.of(userNs,   systemScope, ImmutableSet.of(systemA)))
        .add(Triple.of(userNs,   allScopes,   ImmutableSet.of(userA, systemA)))
        .add(Triple.of(systemNs, noScopes,    ImmutableSet.of()))
        .add(Triple.of(systemNs, userScope,   ImmutableSet.of()))
        .add(Triple.of(systemNs, systemScope, ImmutableSet.of(systemA)))
        .add(Triple.of(systemNs, allScopes,   ImmutableSet.of(systemA)))
      .build();
    for (Triple<Optional<NamespaceId>, Set<EntityScope>, Set<MetadataDataset.SearchTerm>> case1 : cases) {
      List<MetadataDataset.SearchTerm> terms = new ArrayList<>();
      Set<MetadataDataset.SearchTerm> expected = case1.getRight();
      MetadataDataset.determineSearchFields(case1.getLeft(), case1.getMiddle(), terms).accept(term);
      Assert.assertEquals(expected.size(), terms.size());
      Assert.assertEquals(expected, ImmutableSet.copyOf(terms));
    }
  }

  @Test
  public void testSearchDifferentEntityScope() throws InterruptedException, TransactionFailureException {
    MetadataEntity sysArtifact = NamespaceId.SYSTEM.artifact("artifact", "1.0").toMetadataEntity();
    MetadataEntity nsArtifact = new ArtifactId("ns1", "artifact", "1.0").toMetadataEntity();
    String multiWordKey = "multiword";
    String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";

    txnl.execute(() -> {
      dataset.addProperty(nsArtifact, multiWordKey, multiWordValue);
      dataset.addProperty(sysArtifact, multiWordKey, multiWordValue);
    });

    MetadataEntry systemArtifactEntry = new MetadataEntry(sysArtifact, multiWordKey, multiWordValue);
    MetadataEntry nsArtifactEntry = new MetadataEntry(nsArtifact, multiWordKey, multiWordValue);

    NamespaceId ns1 = new NamespaceId("ns1");
    txnl.execute(() -> {
      SearchRequest request = new SearchRequest(ns1, "aV5", ALL_TYPES,
                                                SortInfo.DEFAULT, 0, Integer.MAX_VALUE, 1, null,
                                                false, EnumSet.of(EntityScope.USER));
      List<MetadataEntry> results = dataset.search(request).getResults();
      // the result should not contain system entities
      Assert.assertEquals(Sets.newHashSet(nsArtifactEntry), Sets.newHashSet(results));
      request = new SearchRequest(ns1, "aV5", ALL_TYPES,
                                  SortInfo.DEFAULT, 0, Integer.MAX_VALUE, 1, null,
                                  false, EnumSet.of(EntityScope.SYSTEM));
      results = dataset.search(request).getResults();
      // the result should not contain user entities
      Assert.assertEquals(Sets.newHashSet(systemArtifactEntry), Sets.newHashSet(results));

      request = new SearchRequest(NamespaceId.SYSTEM, "aV5", ALL_TYPES,
                                  SortInfo.DEFAULT, 0, Integer.MAX_VALUE, 1, null,
                                  false, EnumSet.of(EntityScope.SYSTEM));
       results = dataset.search(request).getResults();
       // the result should not contain user entities
       Assert.assertEquals(Sets.newHashSet(systemArtifactEntry), Sets.newHashSet(results));

      request = new SearchRequest(ns1, "aV5", ALL_TYPES,
                                  SortInfo.DEFAULT, 0, Integer.MAX_VALUE, 1, null,
                                  false, EnumSet.allOf(EntityScope.class));
      results = dataset.search(request).getResults();
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
      dataset.addProperty(program1, "key1", "value1");
      dataset.addProperty(program1, "key2", "value2");
      dataset.addTags(program1, "tag1", "tag2");
    });
    txnl.execute(() -> {
      Assert.assertEquals(ImmutableList.of(new MetadataEntry(program1, "key1", "value1")),
                          searchByDefaultIndex(program1.getValue(MetadataEntity.NAMESPACE), "value1",
                                               Collections.emptySet()));
      Assert.assertEquals(ImmutableList.of(new MetadataEntry(program1, "key2", "value2")),
                          searchByDefaultIndex(program1.getValue(MetadataEntity.NAMESPACE), "value2",
                                               Collections.emptySet()));
      Assert.assertEquals(ImmutableList.of(new MetadataEntry(program1, MetadataConstants.TAGS_KEY, "tag1,tag2")),
                          searchByDefaultIndex(program1.getValue(MetadataEntity.NAMESPACE), "tag2",
                                               Collections.emptySet()));
    });

    // Update key1
    txnl.execute(() -> {
      dataset.addProperty(program1, "key1", "value3");
      dataset.removeProperties(program1, "key2");
      dataset.removeTags(program1, "tag2");
    });

    txnl.execute(() -> {
      // Searching for value1 should be empty
      Assert.assertEquals(ImmutableList.of(),
                          searchByDefaultIndex(program1.getValue(MetadataEntity.NAMESPACE), "value1",
                                               Collections.emptySet()));
      // Instead key1 has value value3 now
      Assert.assertEquals(ImmutableList.of(new MetadataEntry(program1, "key1", "value3")),
                          searchByDefaultIndex(program1.getValue(MetadataEntity.NAMESPACE), "value3",
                                               Collections.emptySet()));
      // key2 was deleted
      Assert.assertEquals(ImmutableList.of(),
                          searchByDefaultIndex(program1.getValue(MetadataEntity.NAMESPACE), "value2",
                                               Collections.emptySet()));
      // tag2 was deleted
      Assert.assertEquals(
        ImmutableList.of(),
        searchByDefaultIndex(program1.getValue(MetadataEntity.NAMESPACE), "tag2", ImmutableSet.of()))
      ;
      Assert.assertEquals(
        ImmutableList.of(new MetadataEntry(program1, MetadataConstants.TAGS_KEY, "tag1")),
        searchByDefaultIndex(program1.getValue(MetadataEntity.NAMESPACE), "tag1", ImmutableSet.of())
      );
    });
  }

  @Test
  public void testMultiGet() throws Exception {
    final Map<MetadataEntity, MetadataDataset.Record> allMetadata = new HashMap<>();
    allMetadata.put(program1, new MetadataDataset.Record(program1,
                                                         ImmutableMap.of("key1", "value1", "key2", "value2"),
                                                         ImmutableSet.of("tag1", "tag2", "tag3")));
    allMetadata.put(dataset1, new MetadataDataset.Record(dataset1,
                                                         ImmutableMap.of("key10", "value10", "key11", "value11"),
                                                         ImmutableSet.of()));
    allMetadata.put(app1, new MetadataDataset.Record(app1,
                                                     ImmutableMap.of("key20", "value20", "key21", "value21"),
                                                     ImmutableSet.of()));
    allMetadata.put(dataset2, new MetadataDataset.Record(dataset2,
                                                         ImmutableMap.of("key30", "value30", "key31", "value31",
                                                                         "key32", "value32"),
                                                         ImmutableSet.of()));
    allMetadata.put(artifact1, new MetadataDataset.Record(artifact1,
                                                          ImmutableMap.of("key40", "value41"),
                                                          ImmutableSet.of()));

    txnl.execute(() -> {
      for (Map.Entry<MetadataEntity, MetadataDataset.Record> entry : allMetadata.entrySet()) {
        MetadataDataset.Record metadata = entry.getValue();
        for (Map.Entry<String, String> props : metadata.getProperties().entrySet()) {
          dataset.addProperty(metadata.getMetadataEntity(), props.getKey(), props.getValue());
        }
        dataset.addTags(metadata.getMetadataEntity(),
                        metadata.getTags().toArray(new String[0]));
      }
    });

    txnl.execute(() -> {
      ImmutableSet<MetadataDataset.Record> expected =
        ImmutableSet.<MetadataDataset.Record>builder()
          .add(allMetadata.get(program1))
          .add(allMetadata.get(app1))
          .build();
      Assert.assertEquals(expected, dataset.getMetadata(ImmutableSet.of(program1, app1)));

      expected =
        ImmutableSet.<MetadataDataset.Record>builder()
          .add(allMetadata.get(dataset2))
          .add(allMetadata.get(dataset1))
          .add(allMetadata.get(artifact1))
          .build();
      Assert.assertEquals(expected, dataset.getMetadata(ImmutableSet.of(dataset2, dataset1, artifact1)));

      expected =
        ImmutableSet.<MetadataDataset.Record>builder()
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
      dataset.addProperty(program1, "key1", "value1");
      dataset.addProperty(program1, "key2", "value2");
      dataset.addTags(program1, "tag1", "tag2");

      dataset.addProperty(app1, "key10", "value10");
      dataset.addProperty(app1, "key12", "value12");
      dataset.addTags(app1, "tag11", "tag12");
    });

    txnl.execute(() -> {
      Assert.assertEquals(ImmutableMap.of("key1", "value1", "key2", "value2"), dataset.getProperties(program1));
      Assert.assertEquals(ImmutableSet.of("tag1", "tag2"), dataset.getTags(program1));
      Assert.assertEquals(ImmutableMap.of("key10", "value10", "key12", "value12"), dataset.getProperties(app1));
      Assert.assertEquals(ImmutableSet.of("tag11", "tag12"), dataset.getTags(app1));
    });

    txnl.execute(() -> {
      // Delete all tags for program1, and delete all properties for app1
      dataset.removeTags(program1);
      dataset.removeProperties(app1);
    });

    txnl.execute(() -> {
      Assert.assertEquals(ImmutableMap.of("key1", "value1", "key2", "value2"), dataset.getProperties(program1));
      Assert.assertEquals(ImmutableSet.of(), dataset.getTags(program1));
      Assert.assertEquals(ImmutableMap.of(), dataset.getProperties(app1));
      Assert.assertEquals(ImmutableSet.of("tag11", "tag12"), dataset.getTags(app1));
    });
  }

  @Test
  public void testHistory() throws Exception {
    doTestHistory(dataset, program1, "f_");
    doTestHistory(dataset, app1, "a_");
    doTestHistory(dataset, dataset1, "d_");
    doTestHistory(dataset, dataset2, "s_");
  }

  @Test
  public void testMultipleIndexes() throws Exception {
    final String value = "value";
    final String body = "body";
    final String schema = Schema.recordOf("schema", Schema.Field.of(body, Schema.of(Schema.Type.BYTES))).toString();
    final String name = "dataset1";
    final long creationTime = System.currentTimeMillis();
    txnl.execute(() -> {
      dataset.addProperty(program1, "key", value);
      dataset.addProperty(program1, MetadataConstants.SCHEMA_KEY, schema);
      dataset.addProperty(dataset1, MetadataConstants.ENTITY_NAME_KEY, name);
      dataset.addProperty(dataset1, MetadataConstants.CREATION_TIME_KEY, String.valueOf(creationTime));
    });
    final String namespaceId = program1.getValue(MetadataEntity.NAMESPACE);
    txnl.execute(() -> {
      // entry with no special indexes
      assertSingleIndex(dataset, MetadataDataset.DEFAULT_INDEX_COLUMN.getColumn(), namespaceId, value);
      assertNoIndexes(dataset, MetadataDataset.ENTITY_NAME_INDEX_COLUMN.getColumn(), namespaceId, value);
      assertNoIndexes(dataset, MetadataDataset.INVERTED_ENTITY_NAME_INDEX_COLUMN.getColumn(), namespaceId, value);
      assertNoIndexes(dataset, MetadataDataset.CREATION_TIME_INDEX_COLUMN.getColumn(), namespaceId, value);
      assertNoIndexes(dataset, MetadataDataset.INVERTED_CREATION_TIME_INDEX_COLUMN.getColumn(), namespaceId, value);
      // entry with a schema
      assertSingleIndex(dataset, MetadataDataset.DEFAULT_INDEX_COLUMN.getColumn(), namespaceId, body);
      assertNoIndexes(dataset, MetadataDataset.ENTITY_NAME_INDEX_COLUMN.getColumn(), namespaceId, body);
      assertNoIndexes(dataset, MetadataDataset.INVERTED_ENTITY_NAME_INDEX_COLUMN.getColumn(), namespaceId, body);
      assertNoIndexes(dataset, MetadataDataset.CREATION_TIME_INDEX_COLUMN.getColumn(), namespaceId, body);
      assertNoIndexes(dataset, MetadataDataset.INVERTED_CREATION_TIME_INDEX_COLUMN.getColumn(), namespaceId, body);
      // entry with entity name
      assertSingleIndex(dataset, MetadataDataset.DEFAULT_INDEX_COLUMN.getColumn(), namespaceId, name);
      assertSingleIndex(dataset, MetadataDataset.ENTITY_NAME_INDEX_COLUMN.getColumn(), namespaceId, name);
      assertNoIndexes(dataset, MetadataDataset.INVERTED_ENTITY_NAME_INDEX_COLUMN.getColumn(), namespaceId, name);
      Indexer indexer = new InvertedValueIndexer();
      String index = Iterables.getOnlyElement(indexer.getIndexes(new MetadataEntry(dataset1, "key", name)));
      assertSingleIndex(dataset, MetadataDataset.INVERTED_ENTITY_NAME_INDEX_COLUMN.getColumn(),
                        namespaceId, index.toLowerCase());
      assertNoIndexes(dataset, MetadataDataset.CREATION_TIME_INDEX_COLUMN.getColumn(), namespaceId, name);
      assertNoIndexes(dataset, MetadataDataset.INVERTED_CREATION_TIME_INDEX_COLUMN.getColumn(), namespaceId, name);
      // entry with creation time
      String time = String.valueOf(creationTime);
      assertSingleIndex(dataset, MetadataDataset.DEFAULT_INDEX_COLUMN.getColumn(), namespaceId, time);
      assertNoIndexes(dataset, MetadataDataset.ENTITY_NAME_INDEX_COLUMN.getColumn(), namespaceId, time);
      assertNoIndexes(dataset, MetadataDataset.INVERTED_ENTITY_NAME_INDEX_COLUMN.getColumn(), namespaceId, time);
      assertSingleIndex(dataset, MetadataDataset.CREATION_TIME_INDEX_COLUMN.getColumn(), namespaceId, time);
      assertNoIndexes(dataset, MetadataDataset.INVERTED_CREATION_TIME_INDEX_COLUMN.getColumn(), namespaceId, time);
      assertSingleIndex(dataset, MetadataDataset.INVERTED_CREATION_TIME_INDEX_COLUMN.getColumn(), namespaceId,
                        String.valueOf(Long.MAX_VALUE - creationTime));
    });
  }

  @Test
  public void testCrossNamespaceDefaultSearch() throws Exception {
    MetadataEntity ns1App = new NamespaceId("ns1").app("a").toMetadataEntity();
    MetadataEntity ns2App = new NamespaceId("ns2").app("a").toMetadataEntity();
    txnl.execute(() -> {
      dataset.addProperty(ns1App, "k1", "v1");
      dataset.addProperty(ns1App, "k2", "v2");
      dataset.addProperty(ns2App, "k1", "v1");
    });

    SearchRequest request1 = new SearchRequest(null, "v1", ALL_TYPES, SortInfo.DEFAULT,
                                               0, 10, 0, null, false, EnumSet.allOf(EntityScope.class));
    SearchResults results = txnl.execute(() -> dataset.search(request1));
    Set<MetadataEntry> actual = new HashSet<>(results.getResults());
    Set<MetadataEntry> expected = new HashSet<>();
    expected.add(new MetadataEntry(ns1App, "k1", "v1"));
    expected.add(new MetadataEntry(ns2App, "k1", "v1"));
    Assert.assertEquals(expected, actual);

    SearchRequest request2 = new SearchRequest(null, "v2", ALL_TYPES, SortInfo.DEFAULT,
                                               0, 10, 0, null, false, EnumSet.allOf(EntityScope.class));
    results = txnl.execute(() -> dataset.search(request2));
    Assert.assertEquals(Collections.singletonList(new MetadataEntry(ns1App, "k2", "v2")), results.getResults());

    SearchRequest star = new SearchRequest(null, "*", ALL_TYPES, SortInfo.DEFAULT,
                                           0, 10, 0, null, false, EnumSet.allOf(EntityScope.class));
    results = txnl.execute(() -> dataset.search(star));
    expected.add(new MetadataEntry(ns1App, "k2", "v2"));
    Assert.assertEquals(expected, new HashSet<>(results.getResults()));
  }

  @Test
  public void testCrossNamespaceSearchPagination() throws Exception {
    ApplicationId ns1app1 = new NamespaceId("ns1").app("a1");
    ApplicationId ns1app2 = new NamespaceId("ns1").app("a2");
    ApplicationId ns1app3 = new NamespaceId("ns1").app("a3");
    ApplicationId ns2app1 = new NamespaceId("ns2").app("a1");
    ApplicationId ns2app2 = new NamespaceId("ns2").app("a2");
    String key = MetadataConstants.ENTITY_NAME_KEY;
    txnl.execute(() -> {
      dataset.addProperty(ns1app1.toMetadataEntity(), key, ns1app1.getApplication());
      dataset.addProperty(ns1app2.toMetadataEntity(), key, ns1app2.getApplication());
      dataset.addProperty(ns1app3.toMetadataEntity(), key, ns1app3.getApplication());
      dataset.addProperty(ns2app1.toMetadataEntity(), key, ns2app1.getApplication());
      dataset.addProperty(ns2app2.toMetadataEntity(), key, ns2app2.getApplication());
    });

    MetadataEntry ns1app1Entry = new MetadataEntry(ns1app1.toMetadataEntity(), key, ns1app1.getApplication());
    MetadataEntry ns1app2Entry = new MetadataEntry(ns1app2.toMetadataEntity(), key, ns1app2.getApplication());
    MetadataEntry ns1app3Entry = new MetadataEntry(ns1app3.toMetadataEntity(), key, ns1app3.getApplication());
    MetadataEntry ns2app1Entry = new MetadataEntry(ns2app1.toMetadataEntity(), key, ns2app1.getApplication());
    MetadataEntry ns2app2Entry = new MetadataEntry(ns2app2.toMetadataEntity(), key, ns2app2.getApplication());
    SortInfo nameAsc = new SortInfo(MetadataConstants.ENTITY_NAME_KEY, SortInfo.SortOrder.ASC);
    // first, get the full ordered list in one page
    SearchRequest request1 = new SearchRequest(null, "*", ALL_TYPES, nameAsc,
                                               0, 10, 1, null, false, EnumSet.allOf(EntityScope.class));
    List<MetadataEntry> actual = txnl.execute(() -> dataset.search(request1).getResults());
    List<MetadataEntry> expected = new ArrayList<>();
    // sorted by name asc
    expected.add(ns1app1Entry);
    expected.add(ns2app1Entry);
    expected.add(ns1app2Entry);
    expected.add(ns2app2Entry);
    expected.add(ns1app3Entry);
    Assert.assertEquals(expected, actual);

    // now search with 3 pages, 2 results per page
    SearchRequest request2 = new SearchRequest(null, "*", ALL_TYPES, nameAsc,
                                               0, 2, 3, null, false, EnumSet.allOf(EntityScope.class));
    SearchResults results = txnl.execute(() -> dataset.search(request2));
    // dataset returns all pages, so results should be in same order
    Assert.assertEquals(expected, results.getResults());

    // check the cursors
    List<String> expectedCursors = new ArrayList<>();
    expectedCursors.add(ns1app2Entry.getValue());
    expectedCursors.add(ns1app3Entry.getValue());
    Assert.assertEquals(expectedCursors, results.getCursors());

    // now search for for 2nd and 3rd pages using the cursor
    SearchRequest request3 = new SearchRequest(null, "*", ALL_TYPES, nameAsc,
                                               0, 2, 3, results.getCursors().get(0), false,
                                               EnumSet.allOf(EntityScope.class));
    results = txnl.execute(() -> dataset.search(request3));
    expected.clear();
    expected.add(ns1app2Entry);
    expected.add(ns2app2Entry);
    expected.add(ns1app3Entry);
    Assert.assertEquals(expected, results.getResults());
    Assert.assertEquals(Collections.singletonList(ns1app3Entry.getValue()), results.getCursors());
  }

  @Test
  public void testCrossNamespaceCustomSearch() throws Exception {
    String appName = "app";
    MetadataEntity ns1App = new NamespaceId("ns1").app(appName).toMetadataEntity();
    MetadataEntity ns2App = new NamespaceId("ns2").app(appName).toMetadataEntity();
    txnl.execute(() -> {
      dataset.addProperty(ns2App, MetadataConstants.ENTITY_NAME_KEY, appName);
      dataset.addProperty(ns1App, MetadataConstants.ENTITY_NAME_KEY, appName);
    });

    SortInfo nameAsc = new SortInfo(MetadataConstants.ENTITY_NAME_KEY, SortInfo.SortOrder.ASC);

    SearchRequest request = new SearchRequest(null, "*", ALL_TYPES, nameAsc,
                                              0, 10, 0, null, false, EnumSet.allOf(EntityScope.class));
    SearchResults results = txnl.execute(() -> dataset.search(request));
    List<MetadataEntry> actual = results.getResults();
    List<MetadataEntry> expected = new ArrayList<>();
    expected.add(new MetadataEntry(ns1App, MetadataConstants.ENTITY_NAME_KEY, appName));
    expected.add(new MetadataEntry(ns2App, MetadataConstants.ENTITY_NAME_KEY, appName));
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testPagination() throws Exception {
    String flowName = "name11";
    String dsName = "name21 name22";
    String appName = "name31 name32 name33";
    txnl.execute(() -> {
      dataset.addProperty(program1, MetadataConstants.ENTITY_NAME_KEY, flowName);
      dataset.addProperty(dataset1, MetadataConstants.ENTITY_NAME_KEY, dsName);
      dataset.addProperty(app1, MetadataConstants.ENTITY_NAME_KEY, appName);
    });
    NamespaceId namespaceId = new NamespaceId(program1.getValue(MetadataEntity.NAMESPACE));
    MetadataEntry flowEntry = new MetadataEntry(program1, MetadataConstants.ENTITY_NAME_KEY, flowName);
    MetadataEntry dsEntry = new MetadataEntry(dataset1, MetadataConstants.ENTITY_NAME_KEY, dsName);
    MetadataEntry appEntry = new MetadataEntry(app1, MetadataConstants.ENTITY_NAME_KEY, appName);
    txnl.execute(() -> {
      // since no sort is to be performed by the dataset, we return all (ignore limit and offset)
      SearchRequest request = new SearchRequest(namespaceId, "name*", ALL_TYPES, SortInfo.DEFAULT, 0, 3, 1, null,
                                                false, EnumSet.allOf(EntityScope.class));
      SearchResults searchResults = dataset.search(request);
      Assert.assertEquals(
        // since default indexer is used:
        // 1 index for flow: 'name11'
        // 3 indexes for dataset: 'name21', 'name21 name22', 'name22'
        // 4 indexes for app: 'name31', 'name31 name32 name33', 'name32', 'name33'
        ImmutableList.of(flowEntry, dsEntry, dsEntry, dsEntry, appEntry, appEntry, appEntry, appEntry),
        searchResults.getResults()
      );
      // ascending sort by name. offset and limit should be respected.
      SortInfo nameAsc = new SortInfo(MetadataConstants.ENTITY_NAME_KEY, SortInfo.SortOrder.ASC);
      // first 2 in ascending order
      request = new SearchRequest(namespaceId, "*", ALL_TYPES, nameAsc, 0, 2, 0, null, false,
                                  EnumSet.allOf(EntityScope.class));
      searchResults = dataset.search(request);
      Assert.assertEquals(ImmutableList.of(flowEntry, dsEntry), searchResults.getResults());
      // return 2 with offset 1 in ascending order
      request = new SearchRequest(namespaceId, "*", ALL_TYPES, nameAsc, 1, 2, 0, null, false,
                                  EnumSet.allOf(EntityScope.class));
      searchResults = dataset.search(request);
      Assert.assertEquals(ImmutableList.of(flowEntry, dsEntry, appEntry), searchResults.getResults());
      // descending sort by name. offset and filter should be respected.
      SortInfo nameDesc = new SortInfo(MetadataConstants.ENTITY_NAME_KEY, SortInfo.SortOrder.DESC);
      // first 2 in descending order
      request = new SearchRequest(namespaceId, "*", ALL_TYPES, nameDesc, 0, 2, 0, null, false,
                                  EnumSet.allOf(EntityScope.class));
      searchResults = dataset.search(request);
      Assert.assertEquals(ImmutableList.of(appEntry, dsEntry), searchResults.getResults());
      // last 1 in descending order
      request = new SearchRequest(namespaceId, "*", ALL_TYPES, nameDesc, 2, 1, 0, null, false,
                                  EnumSet.allOf(EntityScope.class));
      searchResults = dataset.search(request);
      Assert.assertEquals(ImmutableList.of(appEntry, dsEntry, flowEntry), searchResults.getResults());
      request = new SearchRequest(namespaceId, "*", ALL_TYPES, nameAsc, 2, 0, 0, null, false,
                                  EnumSet.allOf(EntityScope.class));
      searchResults = dataset.search(request);
      Assert.assertEquals(ImmutableList.of(flowEntry, dsEntry), searchResults.getResults());
      request = new SearchRequest(namespaceId, "*", ALL_TYPES, nameDesc, 1, 0, 0, null, false,
                                  EnumSet.allOf(EntityScope.class));
      searchResults = dataset.search(request);
      Assert.assertEquals(ImmutableList.of(appEntry), searchResults.getResults());
      request = new SearchRequest(namespaceId, "*", ALL_TYPES, nameAsc, 4, 0, 0, null, false,
                                  EnumSet.allOf(EntityScope.class));
      searchResults = dataset.search(request);
      Assert.assertEquals(ImmutableList.of(flowEntry, dsEntry, appEntry), searchResults.getResults());
      request = new SearchRequest(namespaceId, "*", ALL_TYPES, nameDesc, 100, 0, 0, null, false,
                                  EnumSet.allOf(EntityScope.class));
      searchResults = dataset.search(request);
      Assert.assertEquals(ImmutableList.of(appEntry, dsEntry, flowEntry), searchResults.getResults());

      // test cursors
      request = new SearchRequest(namespaceId, "*", ALL_TYPES, nameAsc, 0, 1, 3, null, false,
                                  EnumSet.allOf(EntityScope.class));
      searchResults = dataset.search(request);
      Assert.assertEquals(ImmutableList.of(flowEntry, dsEntry, appEntry), searchResults.getResults());
      Assert.assertEquals(ImmutableList.of(dsName, appName), searchResults.getCursors());

      request = new SearchRequest(namespaceId, "*", ALL_TYPES, nameAsc, 0, 1, 3, searchResults.getCursors().get(0),
                                  false, EnumSet.allOf(EntityScope.class));
      searchResults = dataset.search(request);
      Assert.assertEquals(ImmutableList.of(dsEntry, appEntry), searchResults.getResults());
      Assert.assertEquals(ImmutableList.of(appName), searchResults.getCursors());

      request = new SearchRequest(namespaceId, "*", ALL_TYPES, nameAsc, 0, 1, 3, searchResults.getCursors().get(0),
                                  false, EnumSet.allOf(EntityScope.class));
      searchResults = dataset.search(request);
      Assert.assertEquals(ImmutableList.of(appEntry), searchResults.getResults());
      Assert.assertEquals(ImmutableList.of(), searchResults.getCursors());

      request = new SearchRequest(namespaceId, "*", ALL_TYPES, nameAsc, 0, 2, 3, null, false,
                                  EnumSet.allOf(EntityScope.class));
      searchResults = dataset.search(request);
      Assert.assertEquals(ImmutableList.of(flowEntry, dsEntry, appEntry), searchResults.getResults());
      Assert.assertEquals(ImmutableList.of(appName), searchResults.getCursors());

      request = new SearchRequest(namespaceId, "*", ALL_TYPES, nameAsc, 3, 1, 2, null, false,
                                  EnumSet.allOf(EntityScope.class));
      searchResults = dataset.search(request);
      Assert.assertEquals(ImmutableList.of(flowEntry, dsEntry, appEntry), searchResults.getResults());
      Assert.assertEquals(ImmutableList.of(), searchResults.getCursors());
    });
  }

  private void assertSingleIndex(final MetadataDataset dataset, final String indexColumn, final String namespaceId,
                                 final String value) {
    final String searchQuery = namespaceId + MetadataConstants.KEYVALUE_SEPARATOR + value;
    try (Scanner scan = dataset.searchByIndex(indexColumn, searchQuery)) {
      Assert.assertNotNull(scan.next());
      Assert.assertNull(scan.next());
    }
  }

  private void assertNoIndexes(final MetadataDataset dataset, String indexColumn, String namespaceId, String value) {
    String searchQuery = namespaceId + MetadataConstants.KEYVALUE_SEPARATOR + value;
    try (Scanner scan = dataset.searchByIndex(indexColumn, searchQuery)) {
      Assert.assertNull(scan.next());
    }
  }

  private void doTestHistory(final MetadataDataset dataset, final MetadataEntity targetId, final String prefix)
    throws Exception {
    TransactionExecutor txnl = dsFrameworkUtil.newInMemoryTransactionExecutor((TransactionAware) dataset);

    // Metadata change history keyed by time in millis the change was made
    final Map<Long, MetadataDataset.Record> expected = new HashMap<>();
    // No history for targetId at the beginning
    txnl.execute(() -> {
      MetadataDataset.Record completeRecord = new MetadataDataset.Record(targetId);
      expected.put(System.currentTimeMillis(), completeRecord);
      // Get history for targetId, should be empty
      Assert.assertEquals(ImmutableSet.of(completeRecord),
                          dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), System.currentTimeMillis()));
      // Also, the metadata itself should be equal to the last recorded snapshot
      Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                 System.currentTimeMillis())),
                          new MetadataDataset.Record(targetId,
                                                     dataset.getProperties(targetId), dataset.getTags(targetId)));
    });

    // Since the key to expected map is time in millis, sleep for a millisecond to make sure the key is distinct
    TimeUnit.MILLISECONDS.sleep(1);

    // Add first record
    final MetadataDataset.Record completeRecord =
      new MetadataDataset.Record(targetId, toProps(prefix, "k1", "v1"), toTags(prefix, "t1", "t2"));
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
                          new MetadataDataset.Record(targetId,
                                                     dataset.getProperties(targetId), dataset.getTags(targetId)));
    });
    TimeUnit.MILLISECONDS.sleep(1);

    txnl.execute(() -> {
      // Add a new property and a tag
      dataset.addProperty(targetId, prefix + "k2", "v2");
      dataset.addTags(targetId, prefix + "t3");
    });
    // Save the complete metadata record at this point
    txnl.execute(() -> {
      MetadataDataset.Record completeRecord1 =
        new MetadataDataset.Record(targetId, toProps(prefix, "k1", "v1", "k2", "v2"), toTags(prefix, "t1", "t2", "t3"));
      long time = System.currentTimeMillis();
      expected.put(time, completeRecord1);
      // Assert the history record with the change
      Assert.assertEquals(ImmutableSet.of(completeRecord1),
                          dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
      // Also, the metadata itself should be equal to the last recorded snapshot
      Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                 System.currentTimeMillis())),
                          new MetadataDataset.Record(targetId,
                                                     dataset.getProperties(targetId), dataset.getTags(targetId)));
    });
    TimeUnit.MILLISECONDS.sleep(1);

    txnl.execute(() -> {
      // Add another property and a tag
      dataset.addProperty(targetId, prefix + "k3", "v3");
      dataset.addTags(targetId, prefix + "t4");
    });
    txnl.execute(() -> {
      // Save the complete metadata record at this point
      MetadataDataset.Record completeRecord12 = new MetadataDataset.Record(targetId, toProps(prefix, "k1", "v1",
                                                                                             "k2", "v2", "k3", "v3"),
                                                                           toTags(prefix, "t1", "t2", "t3", "t4"));
      long time = System.currentTimeMillis();
      expected.put(time, completeRecord12);
      // Assert the history record with the change
      Assert.assertEquals(ImmutableSet.of(completeRecord12),
                          dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
      // Also, the metadata itself should be equal to the last recorded snapshot
      Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                 System.currentTimeMillis())),
                          new MetadataDataset.Record(targetId,
                                                     dataset.getProperties(targetId), dataset.getTags(targetId)));
    });
    TimeUnit.MILLISECONDS.sleep(1);

    txnl.execute(() -> {
      // Add the same property and tag as second time
      dataset.addProperty(targetId, prefix + "k2", "v2");
      dataset.addTags(targetId, prefix + "t3");
    });
    txnl.execute(() -> {
      // Save the complete metadata record at this point
      MetadataDataset.Record completeRecord13 = new MetadataDataset.Record(targetId, toProps(prefix, "k1", "v1",
                                                                                             "k2", "v2", "k3", "v3"),
                                                                           toTags(prefix, "t1", "t2", "t3", "t4"));
      long time = System.currentTimeMillis();
      expected.put(time, completeRecord13);
      // Assert the history record with the change
      Assert.assertEquals(ImmutableSet.of(completeRecord13),
                          dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
      // Also, the metadata itself should be equal to the last recorded snapshot
      Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                 System.currentTimeMillis())),
                          new MetadataDataset.Record(targetId,
                                                     dataset.getProperties(targetId), dataset.getTags(targetId)));
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
      MetadataDataset.Record completeRecord14 = new MetadataDataset.Record(targetId,
                                                                           toProps(prefix, "k1", "v1", "k3", "v3"),
                                                                           toTags(prefix, "t1", "t3"));
      long time = System.currentTimeMillis();
      expected.put(time, completeRecord14);
      // Assert the history record with the change
      Assert.assertEquals(ImmutableSet.of(completeRecord14),
                          dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
      // Also, the metadata itself should be equal to the last recorded snapshot
      Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                 System.currentTimeMillis())),
                          new MetadataDataset.Record(targetId,
                                                     dataset.getProperties(targetId), dataset.getTags(targetId)));
    });
    TimeUnit.MILLISECONDS.sleep(1);

    txnl.execute(() -> {
      // Remove all properties and all tags
      dataset.removeProperties(targetId);
      dataset.removeTags(targetId);
    });
    txnl.execute(() -> {
      // Save the complete metadata record at this point
      MetadataDataset.Record completeRecord15 = new MetadataDataset.Record(targetId);
      long time = System.currentTimeMillis();
      expected.put(time, completeRecord15);
      // Assert the history record with the change
      Assert.assertEquals(ImmutableSet.of(completeRecord15),
                          dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), time));
      // Also, the metadata itself should be equal to the last recorded snapshot
      Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                 System.currentTimeMillis())),
                          new MetadataDataset.Record(targetId,
                                                     dataset.getProperties(targetId), dataset.getTags(targetId)));
    });
    TimeUnit.MILLISECONDS.sleep(1);

    // Add one more property and a tag
    txnl.execute(() -> {
      dataset.addProperty(targetId, prefix + "k2", "v2");
      dataset.addTags(targetId, prefix + "t2");
    });
    final MetadataDataset.Record lastCompleteRecord = new MetadataDataset.Record(targetId, toProps(prefix, "k2", "v2"),
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
                          new MetadataDataset.Record(targetId,
                                                     dataset.getProperties(targetId), dataset.getTags(targetId)));
    });
    TimeUnit.MILLISECONDS.sleep(1);

    // Now assert all history
    txnl.execute(() -> {
      for (Map.Entry<Long, MetadataDataset.Record> entry : expected.entrySet()) {
        Assert.assertEquals(entry.getValue(),
                            getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), entry.getKey())));
      }
      // Asserting for current time should give the latest record
      Assert.assertEquals(ImmutableSet.of(lastCompleteRecord),
                          dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId), System.currentTimeMillis()));
      // Also, the metadata itself should be equal to the last recorded snapshot
      Assert.assertEquals(getFirst(dataset.getSnapshotBeforeTime(ImmutableSet.of(targetId),
                                                                 System.currentTimeMillis())),
                          new MetadataDataset.Record(targetId,
                                                     dataset.getProperties(targetId), dataset.getTags(targetId)));
    });
  }

  private void addMetadataHistory(MetadataDataset dataset, MetadataDataset.Record record) {
    for (Map.Entry<String, String> entry : record.getProperties().entrySet()) {
      dataset.addProperty(record.getMetadataEntity(), entry.getKey(), entry.getValue());
    }
    //noinspection ToArrayCallWithZeroLengthArrayArgument
    dataset.addTags(record.getMetadataEntity(), record.getTags().toArray(new String[0]));
  }

  private Map<String, String> toProps(String prefix, String k1, String v1) {
    return ImmutableMap.of(prefix + k1, v1);
  }

  @SuppressWarnings("SameParameterValue")
  private Map<String, String> toProps(String prefix, String k1, String v1, String k2, String v2) {
    return ImmutableMap.of(prefix + k1, v1, prefix + k2, v2);
  }

  @SuppressWarnings("SameParameterValue")
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
                                                   Set<String> types) throws BadRequestException {
    return searchByDefaultIndex(dataset, namespaceId, searchQuery, types);
  }

  // should be called inside a transaction. This method does not start a transaction on its own, so that you can
  // have multiple invocations in the same transaction.
  private List<MetadataEntry> searchByDefaultIndex(MetadataDataset dataset, String namespaceId, String searchQuery,
                                                   Set<String> types) throws BadRequestException {
    SearchRequest request = new SearchRequest(new NamespaceId(namespaceId), searchQuery, types, SortInfo.DEFAULT,
                                              0, Integer.MAX_VALUE, 1, null, false, EnumSet.allOf(EntityScope.class));
    return dataset.search(request).getResults();
  }
}
