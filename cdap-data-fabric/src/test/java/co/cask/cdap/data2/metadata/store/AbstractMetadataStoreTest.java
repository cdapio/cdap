/*
 * Copyright © 2019 Cask Data, Inc.
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

import co.cask.cdap.api.metadata.Metadata;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.audit.InMemoryAuditPublisher;
import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.dataset.SearchRequest;
import co.cask.cdap.data2.metadata.dataset.SortInfo;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.audit.payload.metadata.MetadataPayload;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link MetadataStore}.
 */
public abstract class AbstractMetadataStoreTest {

  protected static Injector injector;
  protected static CConfiguration cConf;
  protected static MetadataStore store;
  private static InMemoryAuditPublisher auditPublisher;

  /**
   * Subclasses must call this after creating the injector and the store.
   * The injector must bind the MetadataStore and InMemoryAuditPublisher.
   */
  static void commonSetup() throws IOException {
    // injector and store must be set up by subclasses.
    cConf = injector.getInstance(CConfiguration.class);
    store = injector.getInstance(MetadataStore.class);
    auditPublisher = injector.getInstance(InMemoryAuditPublisher.class);
    store.createIndex();
  }

  static void commonTearDown() throws IOException {
    store.dropIndex();
  }

  @Before
  public void clearAudit() {
    auditPublisher.popMessages();
  }

  @After
  public void clearMetadata() {
    MetadataSearchResponse response = store.search(
      new SearchRequest(null, "*", EnumSet.allOf(EntityTypeSimpleName.class), SortInfo.DEFAULT,
                        0, Integer.MAX_VALUE, 0, null, true, EnumSet.allOf(EntityScope.class)));
    response.getResults().forEach(result -> store.removeMetadata(result.getMetadataEntity()));
  }

  private static final Map<String, String> EMPTY_PROPERTIES = Collections.emptyMap();
  private static final Set<String> EMPTY_TAGS = Collections.emptySet();
  private static final Map<MetadataScope, Metadata> EMPTY_USER_METADATA =
    ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, EMPTY_TAGS));

  private final ApplicationId app = NamespaceId.DEFAULT.app("app");
  private final ProgramId service = app.service("service");
  private final DatasetId dataset = NamespaceId.DEFAULT.dataset("ds");
  private final Set<String> datasetTags = ImmutableSet.of("dTag");
  private final Map<String, String> appProperties = ImmutableMap.of("aKey", "aValue");
  private final Set<String> appTags = ImmutableSet.of("aTag");
  private final Set<String> tags = ImmutableSet.of("fTag");

  private final AuditMessage auditMessage1 = new AuditMessage(
    0, dataset, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      EMPTY_USER_METADATA, ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, datasetTags)),
      EMPTY_USER_METADATA
    )
  );
  private final AuditMessage auditMessage2 = new AuditMessage(
    0, app, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      EMPTY_USER_METADATA, ImmutableMap.of(MetadataScope.USER, new Metadata(appProperties, EMPTY_TAGS)),
      EMPTY_USER_METADATA
    )
  );
  private final AuditMessage auditMessage3 = new AuditMessage(
    0, app, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      ImmutableMap.of(MetadataScope.USER, new Metadata(appProperties, EMPTY_TAGS)),
      ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, appTags)),
      EMPTY_USER_METADATA
    )
  );
  private final AuditMessage auditMessage7 = new AuditMessage(
    0, service, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      EMPTY_USER_METADATA,
      ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, tags)),
      EMPTY_USER_METADATA
    )
  );
  private final AuditMessage auditMessage8 = new AuditMessage(
    0, service, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, tags)),
      EMPTY_USER_METADATA,
      ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, tags))
    )
  );
  private final AuditMessage auditMessage9 = new AuditMessage(
    0, dataset, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, datasetTags)),
      EMPTY_USER_METADATA,
      ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, datasetTags))
    )
  );
  private final AuditMessage auditMessage11 = new AuditMessage(
    0, app, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      ImmutableMap.of(MetadataScope.USER, new Metadata(appProperties, appTags)),
      EMPTY_USER_METADATA,
      ImmutableMap.of(MetadataScope.USER, new Metadata(appProperties, appTags))
    )
  );
  private final List<AuditMessage> expectedAuditMessages = ImmutableList.of(
    auditMessage1, auditMessage2, auditMessage3, auditMessage7,
    auditMessage8, auditMessage9, auditMessage11
  );

  @Test
  public void testPublishing() {
    generateMetadataUpdates();

    // Audit messages for metadata changes
    List<AuditMessage> actualAuditMessages = new ArrayList<>();
    String systemNs = NamespaceId.SYSTEM.getNamespace();
    for (AuditMessage auditMessage : auditPublisher.popMessages()) {
      // Ignore system audit messages
      if (auditMessage.getType() == AuditType.METADATA_CHANGE) {
        if (!systemNs.equalsIgnoreCase(auditMessage.getEntity().getValue(MetadataEntity.NAMESPACE))) {
          actualAuditMessages.add(auditMessage);
        }
      }
    }
    Assert.assertEquals(expectedAuditMessages, actualAuditMessages);
  }

  @Test
  public void testPublishingDisabled() {
    boolean auditEnabled = cConf.getBoolean(Constants.Audit.ENABLED);
    cConf.setBoolean(Constants.Audit.ENABLED, false);
    generateMetadataUpdates();

    try {
      List<AuditMessage> publishedAuditMessages = auditPublisher.popMessages();
      Assert.fail(String.format("Expected no changes to be published, but found %d changes: %s.",
                                publishedAuditMessages.size(), publishedAuditMessages));
    } catch (AssertionError e) {
      // expected
    }
    // reset config
    cConf.setBoolean(Constants.Audit.ENABLED, auditEnabled);
  }

  // this tests the replaceMetadata() method, especially for the case when it is called repeatedly
  @Test
  public void testSystemMetadata() {
    MetadataEntity entity = NamespaceId.DEFAULT.app("appX").workflow("wtf").toMetadataEntity();
    store.replaceMetadata(MetadataScope.SYSTEM,
                          new MetadataDataset.Record(
                            entity,
                            ImmutableMap.of("a", "b", "x", "y"),
                            ImmutableSet.of("tag1", "tag2")),
                          ImmutableSet.of("a"), ImmutableSet.of("x"));
    MetadataSearchResponse response = store.search(
      new SearchRequest(null, "tag1", Collections.emptySet(),
                        SortInfo.DEFAULT, 0, Integer.MAX_VALUE, 0, null, false, EnumSet.allOf(EntityScope.class)));
    Set<MetadataSearchResultRecord> results = response.getResults();
    Assert.assertEquals(1, results.size());

    store.replaceMetadata(MetadataScope.SYSTEM,
                          new MetadataDataset.Record(
                            entity,
                            ImmutableMap.of("a", "b", "x", "y"),
                            ImmutableSet.of("tag1", "tag2")),
                          ImmutableSet.of("a"), ImmutableSet.of("x"));
    response = store.search(
      new SearchRequest(null, "tag1", Collections.emptySet(),
                        SortInfo.DEFAULT, 0, Integer.MAX_VALUE, 0, null, false, EnumSet.allOf(EntityScope.class)));
    results = response.getResults();
    Assert.assertEquals(1, results.size());
  }

  @Test
  public void testSearchOnTagsUpdate() {
    MetadataEntity entity = NamespaceId.DEFAULT.app("appX").workflow("wtf").toMetadataEntity();
    store.addTags(MetadataScope.SYSTEM, entity, ImmutableSet.of("tag1", "tag2"));
    Assert.assertEquals(ImmutableSet.of("tag1", "tag2"), store.getTags(MetadataScope.SYSTEM, entity));
    MetadataSearchResponse response = store.search(
      new SearchRequest(null, "tag1", Collections.emptySet(),
                        SortInfo.DEFAULT, 0, Integer.MAX_VALUE, 0, null, false, EnumSet.allOf(EntityScope.class)));
    Set<MetadataSearchResultRecord> results = response.getResults();
    Assert.assertEquals(1, results.size());

    // add an more tags
    store.addTags(MetadataScope.SYSTEM, entity, ImmutableSet.of("tag3", "tag4"));
    ImmutableSet<String> expectedTags = ImmutableSet.of("tag1", "tag2", "tag3", "tag4");
    Assert.assertEquals(expectedTags, store.getTags(MetadataScope.SYSTEM, entity));
    for (String expectedTag : expectedTags) {
      response = store.search(
        new SearchRequest(null, expectedTag, Collections.emptySet(),
                          SortInfo.DEFAULT, 0, Integer.MAX_VALUE, 0, null, false, EnumSet.allOf(EntityScope.class)));
      results = response.getResults();
      Assert.assertEquals(1, results.size());

    }

    // add an empty set of tags. This should have no effect on retrieval or search of tags
    store.addTags(MetadataScope.SYSTEM, entity, ImmutableSet.of());
    Assert.assertEquals(expectedTags, store.getTags(MetadataScope.SYSTEM, entity));
    response = store.search(
      new SearchRequest(null, "tag1", Collections.emptySet(),
                        SortInfo.DEFAULT, 0, Integer.MAX_VALUE, 0, null, false, EnumSet.allOf(EntityScope.class)));
    results = response.getResults();
    Assert.assertEquals(1, results.size());
  }

  @Test
  public void testSearchWeight() {
    ProgramId service1 = new ProgramId("ns1", "app1", ProgramType.SERVICE, "service1");
    DatasetId dataset1 = new DatasetId("ns1", "ds1");
    DatasetId dataset2 = new DatasetId("ns1", "ds2");

    // Add metadata
    String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";
    Map<String, String> userProps = ImmutableMap.of("key1", "value1",
                                                    "key2", "value2",
                                                    "multiword", multiWordValue);
    Map<String, String> systemProps = ImmutableMap.of("sysKey1", "sysValue1");
    Set<String> userTags = ImmutableSet.of("tag1", "tag2");
    Set<String> streamUserTags = ImmutableSet.of("tag3", "tag4");
    Set<String> sysTags = ImmutableSet.of("sysTag1");
    store.addProperties(MetadataScope.USER, service1.toMetadataEntity(), userProps);
    store.addProperties(MetadataScope.SYSTEM, service1.toMetadataEntity(), systemProps);
    store.addTags(MetadataScope.USER, service1.toMetadataEntity(), userTags);
    store.addTags(MetadataScope.SYSTEM, service1.toMetadataEntity(), sysTags);
    store.addTags(MetadataScope.USER, dataset2.toMetadataEntity(), streamUserTags);
    store.removeTags(MetadataScope.USER, dataset2.toMetadataEntity(), streamUserTags);
    store.addProperties(MetadataScope.USER, dataset2.toMetadataEntity(), userProps);
    store.removeProperties(MetadataScope.USER, dataset2.toMetadataEntity(),
                           ImmutableSet.of("key1", "key2", "multiword"));

    Map<String, String> streamUserProps = ImmutableMap.of("sKey1", "sValue1 sValue2",
                                                          "Key1", "Value1");
    store.addProperties(MetadataScope.USER, dataset2.toMetadataEntity(), streamUserProps);

    Map<String, String> datasetUserProps = ImmutableMap.of("sKey1", "sValuee1 sValuee2");
    store.addProperties(MetadataScope.USER, dataset1.toMetadataEntity(), datasetUserProps);

    // Test score and metadata match
    MetadataSearchResponse response = search("ns1", "value1 multiword:av2");
    Assert.assertEquals(2, response.getTotal());
    List<MetadataSearchResultRecord> actual = Lists.newArrayList(response.getResults());

    Map<MetadataScope, Metadata> expectedMetadata =
      ImmutableMap.of(MetadataScope.USER, new Metadata(userProps, userTags),
                      MetadataScope.SYSTEM, new Metadata(systemProps, sysTags));
    Map<MetadataScope, Metadata> expectedStreamMetadata =
      ImmutableMap.of(MetadataScope.USER, new Metadata(streamUserProps, Collections.emptySet()));
    Map<MetadataScope, Metadata> expectedDatasetMetadata =
      ImmutableMap.of(MetadataScope.USER, new Metadata(datasetUserProps, Collections.emptySet()));
    List<MetadataSearchResultRecord> expected =
      Lists.newArrayList(
        new MetadataSearchResultRecord(service1,
                                       expectedMetadata),
        new MetadataSearchResultRecord(dataset2,
                                       expectedStreamMetadata)
      );
    Assert.assertEquals(expected, actual);

    response = search("ns1", "value1 sValue*");
    Assert.assertEquals(3, response.getTotal());
    actual = Lists.newArrayList(response.getResults());
    expected = Lists.newArrayList(
      new MetadataSearchResultRecord(dataset2,
                                     expectedStreamMetadata),
      new MetadataSearchResultRecord(dataset1,
                                     expectedDatasetMetadata),
      new MetadataSearchResultRecord(service1,
                                     expectedMetadata)
    );
    Assert.assertEquals(expected, actual);

    response = search("ns1", "*");
    Assert.assertEquals(3, response.getTotal());
    actual = Lists.newArrayList(response.getResults());
    Assert.assertTrue(actual.containsAll(expected));
  }

  @Test
  public void testCrossNamespaceSearch() {
    NamespaceId ns1 = new NamespaceId("ns1");
    NamespaceId ns2 = new NamespaceId("ns2");

    MetadataEntity ns1app1 = ns1.app("a1").toMetadataEntity();
    MetadataEntity ns1app2 = ns1.app("a2").toMetadataEntity();
    MetadataEntity ns1app3 = ns1.app("a3").toMetadataEntity();
    MetadataEntity ns2app1 = ns2.app("a1").toMetadataEntity();
    MetadataEntity ns2app2 = ns2.app("a2").toMetadataEntity();

    store.addProperty(MetadataScope.USER, ns1app1, "k1", "v1");
    store.addTags(MetadataScope.USER, ns1app1, Collections.singleton("v1"));
    Metadata meta = new Metadata(Collections.singletonMap("k1", "v1"), Collections.singleton("v1"));
    MetadataSearchResultRecord ns1app1Record =
      new MetadataSearchResultRecord(ns1app1, Collections.singletonMap(MetadataScope.USER, meta));

    store.addProperty(MetadataScope.USER, ns1app2, "k1", "v1");
    store.addProperty(MetadataScope.USER, ns1app2, "k2", "v2");
    meta = new Metadata(ImmutableMap.of("k1", "v1", "k2", "v2"), Collections.emptySet());
    MetadataSearchResultRecord ns1app2Record =
      new MetadataSearchResultRecord(ns1app2, Collections.singletonMap(MetadataScope.USER, meta));

    store.addProperty(MetadataScope.USER, ns1app3, "k1", "v1");
    store.addProperty(MetadataScope.USER, ns1app3, "k3", "v3");
    meta = new Metadata(ImmutableMap.of("k1", "v1", "k3", "v3"), Collections.emptySet());
    MetadataSearchResultRecord ns1app3Record =
      new MetadataSearchResultRecord(ns1app3, Collections.singletonMap(MetadataScope.USER, meta));

    store.addProperties(MetadataScope.USER, ImmutableMap.of(
      ns2app1, ImmutableMap.of("k1", "v1", "k2", "v2"),
      ns2app2, ImmutableMap.of("k1", "v1")));
    store.addTags(MetadataScope.USER, ns2app2, ImmutableSet.of("v2", "v3"));

    meta = new Metadata(ImmutableMap.of("k1", "v1", "k2", "v2"), Collections.emptySet());
    MetadataSearchResultRecord ns2app1Record =
      new MetadataSearchResultRecord(ns2app1, Collections.singletonMap(MetadataScope.USER, meta));
    meta = new Metadata(ImmutableMap.of("k1", "v1"), ImmutableSet.of("v2", "v3"));
    MetadataSearchResultRecord ns2app2Record =
      new MetadataSearchResultRecord(ns2app2, Collections.singletonMap(MetadataScope.USER, meta));

    // everything should match 'v1'
    SearchRequest request = new SearchRequest(null, "v1", EnumSet.allOf(EntityTypeSimpleName.class), SortInfo.DEFAULT,
                                              0, 10, 0, null, false, EnumSet.allOf(EntityScope.class));
    MetadataSearchResponse results = store.search(request);
    Set<MetadataSearchResultRecord> expected = new HashSet<>();
    expected.add(ns1app1Record);
    expected.add(ns1app2Record);
    expected.add(ns1app3Record);
    expected.add(ns2app1Record);
    expected.add(ns2app2Record);
    Assert.assertEquals(expected, results.getResults());

    // ns1app2, ns2app1, and ns2app2 should match 'v2'
    request = new SearchRequest(null, "v2", EnumSet.allOf(EntityTypeSimpleName.class), SortInfo.DEFAULT,
                                0, 10, 0, null, false, EnumSet.allOf(EntityScope.class));
    results = store.search(request);
    expected.clear();
    expected.add(ns1app2Record);
    expected.add(ns2app1Record);
    expected.add(ns2app2Record);
    Assert.assertEquals(expected, results.getResults());

    // ns1app3 and ns2app2 should match 'v3'
    request = new SearchRequest(null, "v3", EnumSet.allOf(EntityTypeSimpleName.class), SortInfo.DEFAULT,
                                0, 10, 0, null, false, EnumSet.allOf(EntityScope.class));
    results = store.search(request);
    expected.clear();
    expected.add(ns1app3Record);
    expected.add(ns2app2Record);
    Assert.assertEquals(expected, results.getResults());
  }

  @Test
  public void testCrossNamespacePagination() {
    NamespaceId ns1 = new NamespaceId("ns1");
    NamespaceId ns2 = new NamespaceId("ns2");

    MetadataEntity ns1app1 = ns1.app("a1").toMetadataEntity();
    MetadataEntity ns1app2 = ns1.app("a2").toMetadataEntity();
    MetadataEntity ns1app3 = ns1.app("a3").toMetadataEntity();
    MetadataEntity ns2app1 = ns2.app("a1").toMetadataEntity();
    MetadataEntity ns2app2 = ns2.app("a2").toMetadataEntity();

    store.addTags(MetadataScope.USER, ns1app1, Collections.singleton("v1"));
    store.addTags(MetadataScope.USER, ns1app2, Collections.singleton("v1"));
    store.addTags(MetadataScope.USER, ns1app3, Collections.singleton("v1"));
    store.addTags(MetadataScope.USER, ns2app1, Collections.singleton("v1"));
    store.addTags(MetadataScope.USER, ns2app2, Collections.singleton("v1"));

    // first get everything
    SearchRequest request = new SearchRequest(null, "*", EnumSet.allOf(EntityTypeSimpleName.class), SortInfo.DEFAULT,
                                              0, 10, 0, null, false, EnumSet.allOf(EntityScope.class));
    MetadataSearchResponse results = store.search(request);
    Assert.assertEquals(5, results.getResults().size());

    Iterator<MetadataSearchResultRecord> resultIter = results.getResults().iterator();

    MetadataSearchResultRecord result1 = resultIter.next();
    MetadataSearchResultRecord result2 = resultIter.next();
    MetadataSearchResultRecord result3 = resultIter.next();
    MetadataSearchResultRecord result4 = resultIter.next();
    MetadataSearchResultRecord result5 = resultIter.next();

    // get 4 results (guaranteed to have at least one from each namespace), offset 1
    request = new SearchRequest(null, "*", EnumSet.allOf(EntityTypeSimpleName.class), SortInfo.DEFAULT,
                                1, 4, 0, null, false, EnumSet.allOf(EntityScope.class));
    results = store.search(request);

    List<MetadataSearchResultRecord> expected = new ArrayList<>();
    expected.add(result2);
    expected.add(result3);
    expected.add(result4);
    expected.add(result5);
    List<MetadataSearchResultRecord> actual = new ArrayList<>(results.getResults());
    Assert.assertEquals(expected, actual);

    // get the first four
    request = new SearchRequest(null, "*", EnumSet.allOf(EntityTypeSimpleName.class), SortInfo.DEFAULT,
                                0, 4, 0, null, false, EnumSet.allOf(EntityScope.class));
    results = store.search(request);
    expected.clear();
    expected.add(result1);
    expected.add(result2);
    expected.add(result3);
    expected.add(result4);
    actual.clear();
    actual.addAll(results.getResults());
    Assert.assertEquals(expected, actual);

    // get middle 3
    request = new SearchRequest(null, "*", EnumSet.allOf(EntityTypeSimpleName.class), SortInfo.DEFAULT,
                                1, 3, 0, null, false, EnumSet.allOf(EntityScope.class));
    results = store.search(request);
    expected.clear();
    expected.add(result2);
    expected.add(result3);
    expected.add(result4);
    actual.clear();
    actual.addAll(results.getResults());
    Assert.assertEquals(expected, actual);
  }

  // Tests pagination for search results queries that do not have indexes stored in sorted order
  // The pagination for these queries is done in DefaultMetadataStore, so adding this test here.
  @Test
  public void testSearchPagination() {
    NamespaceId nsId = new NamespaceId("ns");
    MetadataEntity service = nsId.app("app").service("service").toMetadataEntity();
    MetadataEntity worker = nsId.app("app2").worker("worker").toMetadataEntity();
    MetadataEntity dataset = nsId.dataset("dataset").toMetadataEntity();
    // add a dummy entity which starts with _ to test that it doesnt show up see: CDAP-7910
    MetadataEntity trackerDataset = nsId.dataset("_auditLog").toMetadataEntity();

    store.addTags(MetadataScope.USER, service, ImmutableSet.of("tag", "tag1"));
    store.addTags(MetadataScope.USER, worker, ImmutableSet.of("tag2", "tag3 tag4"));
    store.addTags(MetadataScope.USER, dataset, ImmutableSet.of("tag5 tag6", "tag7 tag8"));
    // add bunch of tags to ensure higher weight to this entity in search result
    store.addTags(MetadataScope.USER, trackerDataset, ImmutableSet.of("tag9", "tag10", "tag11", "tag12", "tag13"));

    MetadataSearchResultRecord serviceSearchResult = new MetadataSearchResultRecord(service);
    MetadataSearchResultRecord workerSearchResult = new MetadataSearchResultRecord(worker);
    MetadataSearchResultRecord datasetSearchResult = new MetadataSearchResultRecord(dataset);
    MetadataSearchResultRecord trackerDatasetSearchResult = new MetadataSearchResultRecord(trackerDataset);

    // relevance order for searchQuery "tag*" is trackerDataset, dataset, worker, service
    // (this depends on how many tags got matched with the search query)
    // trackerDataset entity should not be part
    MetadataSearchResponse response = search(nsId.getNamespace(), "tag*", 0, Integer.MAX_VALUE, 1);
    Assert.assertEquals(3, response.getTotal());
    Assert.assertEquals(
      ImmutableList.of(datasetSearchResult, workerSearchResult, serviceSearchResult),
      ImmutableList.copyOf(stripMetadata(response.getResults()))
    );

    // trackerDataset entity should be be part since showHidden is true
    response = search(nsId.getNamespace(), "tag*", 0, Integer.MAX_VALUE, 1, true);
    Assert.assertEquals(4, response.getTotal());
    Assert.assertEquals(
      ImmutableSet.of(trackerDatasetSearchResult, datasetSearchResult, workerSearchResult, serviceSearchResult),
      ImmutableSet.copyOf(stripMetadata(response.getResults()))
    );

    response = search(nsId.getNamespace(), "tag*", 0, 2, 1);
    Assert.assertEquals(3, response.getTotal());
    Assert.assertEquals(
      ImmutableList.of(datasetSearchResult, workerSearchResult),
      ImmutableList.copyOf(stripMetadata(response.getResults()))
    );

    // skipping trackerDataset should not affect the offset
    response = search(nsId.getNamespace(), "tag*", 1, 2, 1);
    Assert.assertEquals(3, response.getTotal());
    Assert.assertEquals(
      ImmutableList.of(workerSearchResult, serviceSearchResult),
      ImmutableList.copyOf(stripMetadata(response.getResults()))
    );

    // if showHidden is true trackerDataset should affect the offset
    response = search(nsId.getNamespace(), "tag*", 1, 3, 1, true);
    Assert.assertEquals(4, response.getTotal());
    Assert.assertEquals(
      ImmutableList.of(datasetSearchResult, workerSearchResult, serviceSearchResult),
      ImmutableList.copyOf(stripMetadata(response.getResults()))
    );

    response = search(nsId.getNamespace(), "tag*", 2, 2, 1);
    Assert.assertEquals(3, response.getTotal());
    Assert.assertEquals(
      ImmutableList.of(serviceSearchResult),
      ImmutableList.copyOf(stripMetadata(response.getResults()))
    );

    response = search(nsId.getNamespace(), "tag*", 4, 2, 1);
    Assert.assertEquals(3, response.getTotal());
    Assert.assertEquals(
      ImmutableList.<MetadataSearchResultRecord>of(),
      ImmutableList.copyOf(stripMetadata(response.getResults()))
    );

    // ensure overflow bug is squashed (JIRA: CDAP-8133)
    response = search(nsId.getNamespace(), "tag*", 1, Integer.MAX_VALUE, 0);
    Assert.assertEquals(3, response.getTotal());
    Assert.assertEquals(
      ImmutableList.of(workerSearchResult, serviceSearchResult),
      ImmutableList.copyOf(stripMetadata(response.getResults()))
    );
  }

  private MetadataSearchResponse search(String ns, String searchQuery) {
    return search(ns, searchQuery, 0, Integer.MAX_VALUE, 0);
  }

  private MetadataSearchResponse search(String ns, String searchQuery,
                                        int offset, int limit, int numCursors) {
    return search(ns, searchQuery, offset, limit, numCursors, false);
  }

  private MetadataSearchResponse search(String ns, String searchQuery,
                                        int offset, int limit, int numCursors, boolean showHidden) {
    SearchRequest request =
      new SearchRequest(new NamespaceId(ns), searchQuery, EnumSet.allOf(EntityTypeSimpleName.class),
                        SortInfo.DEFAULT, offset, limit, numCursors, "", showHidden, EnumSet.allOf(EntityScope.class));
    return store.search(request);
  }

  private void generateMetadataUpdates() {
    store.addTags(MetadataScope.USER, dataset.toMetadataEntity(), datasetTags);
    store.addProperties(MetadataScope.USER, app.toMetadataEntity(), appProperties);
    store.addTags(MetadataScope.USER, app.toMetadataEntity(), appTags);
    store.addTags(MetadataScope.USER, service.toMetadataEntity(), tags);
    store.removeTags(MetadataScope.USER, service.toMetadataEntity());
    store.removeTags(MetadataScope.USER, dataset.toMetadataEntity(), datasetTags);
    store.removeMetadata(MetadataScope.USER, app.toMetadataEntity());
  }

  private Set<MetadataSearchResultRecord> stripMetadata(Set<MetadataSearchResultRecord> searchResultsWithMetadata) {
    Set<MetadataSearchResultRecord> metadataStrippedResults = new LinkedHashSet<>(searchResultsWithMetadata.size());
    for (MetadataSearchResultRecord searchResultWithMetadata : searchResultsWithMetadata) {
      metadataStrippedResults.add(new MetadataSearchResultRecord(searchResultWithMetadata.getEntityId()));
    }
    return metadataStrippedResults;
  }
}
