/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.store;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.metadata.publisher.MetadataKafkaTestBase;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.metadata.MetadataChangeRecord;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.tephra.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link MetadataStore}
 */
public class MetadataStoreTest extends MetadataKafkaTestBase {

  private final Id.Application app = Id.Application.from(Id.Namespace.DEFAULT, "app");
  private final Id.Program flow = Id.Program.from(app, ProgramType.FLOW, "flow");
  private final Id.DatasetInstance dataset = Id.DatasetInstance.from(Id.Namespace.DEFAULT, "ds");
  private final Id.Stream stream = Id.Stream.from(Id.Namespace.DEFAULT, "stream");
  private final Set<String> datasetTags = ImmutableSet.of("dTag");
  private final Map<String, String> appProperties = ImmutableMap.of("aKey", "aValue");
  private final Set<String> appTags = ImmutableSet.of("aTag");
  private final Map<String, String> streamProperties = ImmutableMap.of("stKey", "stValue");
  private final Map<String, String> updatedStreamProperties = ImmutableMap.of("stKey", "stV");
  private final Set<String> flowTags = ImmutableSet.of("fTag");

  private final MetadataChangeRecord change1 = new MetadataChangeRecord(
    new MetadataRecord(dataset),
    new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(dataset, ImmutableMap.<String, String>of(), datasetTags),
      new MetadataRecord(dataset)
    ),
    System.currentTimeMillis()
  );
  private final MetadataChangeRecord change2 = new MetadataChangeRecord(
    new MetadataRecord(app),
    new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(app, appProperties, ImmutableSet.<String>of()),
      new MetadataRecord(app)
    ),
    System.currentTimeMillis()
  );
  private final MetadataChangeRecord change3 = new MetadataChangeRecord(
    new MetadataRecord(app, appProperties, ImmutableSet.<String>of()),
    new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(app, ImmutableMap.<String, String>of(), appTags),
      new MetadataRecord(app)
    ),
    System.currentTimeMillis()
  );
  private final MetadataChangeRecord change4 = new MetadataChangeRecord(
    new MetadataRecord(stream),
    new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(stream, streamProperties, ImmutableSet.<String>of()),
      new MetadataRecord(stream)
    ),
    System.currentTimeMillis()
  );
  private final MetadataChangeRecord change5 = new MetadataChangeRecord(
    new MetadataRecord(stream, streamProperties, ImmutableSet.<String>of()),
    new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(stream),
      new MetadataRecord(stream)
    ),
    System.currentTimeMillis()
  );
  private final MetadataChangeRecord change6 = new MetadataChangeRecord(
    new MetadataRecord(stream, streamProperties, ImmutableSet.<String>of()),
    new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(stream, updatedStreamProperties, ImmutableSet.<String>of()),
      new MetadataRecord(stream, streamProperties, ImmutableSet.<String>of())
    ),
    System.currentTimeMillis()
  );
  private final MetadataChangeRecord change7 = new MetadataChangeRecord(
    new MetadataRecord(flow),
    new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(flow, ImmutableMap.<String, String>of(), flowTags),
      new MetadataRecord(flow)
    ),
    System.currentTimeMillis()
  );
  private final MetadataChangeRecord change8 = new MetadataChangeRecord(
    new MetadataRecord(flow, ImmutableMap.<String, String>of(), flowTags),
    new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(flow),
      new MetadataRecord(flow, ImmutableMap.<String, String>of(), flowTags)
    ),
    System.currentTimeMillis()
  );
  private final MetadataChangeRecord change9 = new MetadataChangeRecord(
    new MetadataRecord(dataset, ImmutableMap.<String, String>of(), datasetTags),
    new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(dataset),
      new MetadataRecord(dataset, ImmutableMap.<String, String>of(), datasetTags)
    ),
    System.currentTimeMillis()
  );
  private final MetadataChangeRecord change10 = new MetadataChangeRecord(
    new MetadataRecord(stream, updatedStreamProperties, ImmutableSet.<String>of()),
    new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(stream),
      new MetadataRecord(stream, updatedStreamProperties, ImmutableSet.<String>of())
    ),
    System.currentTimeMillis()
  );
  private final MetadataChangeRecord change11 = new MetadataChangeRecord(
    new MetadataRecord(app, appProperties, appTags),
    new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(app, ImmutableMap.<String, String>of(), ImmutableSet.<String>of()),
      new MetadataRecord(app, appProperties, appTags)
    ),
    System.currentTimeMillis()
  );
  private final List<MetadataChangeRecord> expectedChanges = ImmutableList.of(
  change1, change2, change3, change4, change5, change6, change7, change8, change9, change10, change11);

  private int kafkaOffset = 0;

  private static TransactionManager txManager;
  private static MetadataStore store;

  @BeforeClass
  public static void setup() throws IOException {
    MetadataKafkaTestBase.setup();
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    store = injector.getInstance(MetadataStore.class);
  }

  @Test
  public void testPublishing() throws InterruptedException {
    generateMetadataUpdates();
    List<MetadataChangeRecord> publishedChanges = getPublishedMetadataChanges(expectedChanges.size());
    for (int i = 0; i < expectedChanges.size(); i++) {
      MetadataChangeRecord expected = expectedChanges.get(i);
      MetadataChangeRecord actual = publishedChanges.get(i);
      Assert.assertEquals(expected.getPrevious(), actual.getPrevious());
      Assert.assertEquals(expected.getChanges(), actual.getChanges());
    }
    // note kafka offset
    kafkaOffset += publishedChanges.size();
  }

  @Test
  public void testPublishingDisabled() throws InterruptedException {
    boolean publishEnabled = cConf.getBoolean(Constants.Metadata.UPDATES_PUBLISH_ENABLED);
    cConf.setBoolean(Constants.Metadata.UPDATES_PUBLISH_ENABLED, false);
    generateMetadataUpdates();

    try {
      List<MetadataChangeRecord> publishedChanges = getPublishedMetadataChanges(expectedChanges.size(), kafkaOffset);
      Assert.fail(String.format("Expected no changes to be published, but found %d changes: %s.",
                                publishedChanges.size(), publishedChanges));
    } catch (AssertionError e) {
      // expected
    }
    // reset config
    cConf.setBoolean(Constants.Metadata.UPDATES_PUBLISH_ENABLED, publishEnabled);
  }

  @Test
  public void testSearchWeight() throws Exception {
    Id.Program flow1 = Id.Program.from("ns1", "app1", ProgramType.FLOW, "flow1");
    Id.Stream stream1 = Id.Stream.from("ns1", "s1");
    Id.DatasetInstance dataset1 = Id.DatasetInstance.from("ns1", "ds1");

    String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";
    store.setProperties(MetadataScope.USER, flow1, ImmutableMap.of("key1", "value1",
                                                                   "key2", "value2",
                                                                   "multiword", multiWordValue));

    store.setProperties(MetadataScope.USER, stream1, ImmutableMap.of("sKey1", "sValue1 sValue2",
                                                                     "Key1", "Value1"));

    store.setProperties(MetadataScope.USER, dataset1, ImmutableMap.of("sKey1", "sValuee1 sValuee2"));

    // Test score match
    List<MetadataSearchResultRecord> actual = Lists.newArrayList(store.searchMetadata("ns1", "value1 multiword:av2"));
    List<MetadataSearchResultRecord> expected = Lists.newArrayList(new MetadataSearchResultRecord(flow1),
                                                                        new MetadataSearchResultRecord(stream1));
    Assert.assertEquals(expected, actual);

    actual = Lists.newArrayList(store.searchMetadata("ns1", "value1 sValue*"));
    expected = Lists.newArrayList(new MetadataSearchResultRecord(stream1),
                                  new MetadataSearchResultRecord(dataset1),
                                  new MetadataSearchResultRecord(flow1));
    Assert.assertEquals(expected, actual);
  }

  @AfterClass
  public static void teardown() {
    MetadataKafkaTestBase.teardown();
    txManager.stopAndWait();
  }

  private void generateMetadataUpdates() {
    store.addTags(MetadataScope.USER, dataset, datasetTags.iterator().next());
    store.setProperties(MetadataScope.USER, app, appProperties);
    store.addTags(MetadataScope.USER, app, appTags.iterator().next());
    store.setProperties(MetadataScope.USER, stream, streamProperties);
    store.setProperties(MetadataScope.USER, stream, streamProperties);
    store.setProperties(MetadataScope.USER, stream, updatedStreamProperties);
    store.addTags(MetadataScope.USER, flow, flowTags.iterator().next());
    store.removeTags(MetadataScope.USER, flow);
    store.removeTags(MetadataScope.USER, dataset, datasetTags.iterator().next());
    store.removeProperties(MetadataScope.USER, stream);
    store.removeMetadata(MetadataScope.USER, app);
  }
}
