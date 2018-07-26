/*
 * Copyright © 2015-2018 Cask Data, Inc.
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
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.data2.audit.InMemoryAuditPublisher;
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
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.metadata.MetadataSearchResponseV2;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecordV2;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionInMemoryModule;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
 * Tests for {@link MetadataStore}
 */
public class DefaultMetadataStoreTest {
  private static final Map<String, String> EMPTY_PROPERTIES = Collections.emptyMap();
  private static final Set<String> EMPTY_TAGS = Collections.emptySet();
  private static final Map<MetadataScope, Metadata> EMPTY_USER_METADATA =
    ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, EMPTY_TAGS));

  private final ApplicationId app = NamespaceId.DEFAULT.app("app");
  private final ProgramId flow = app.flow("flow");
  private final DatasetId dataset = NamespaceId.DEFAULT.dataset("ds");
  private final StreamId stream = NamespaceId.DEFAULT.stream("stream");
  private final Set<String> datasetTags = ImmutableSet.of("dTag");
  private final Map<String, String> appProperties = ImmutableMap.of("aKey", "aValue");
  private final Set<String> appTags = ImmutableSet.of("aTag");
  private final Map<String, String> streamProperties = ImmutableMap.of("stKey", "stValue");
  private final Map<String, String> updatedStreamProperties = ImmutableMap.of("stKey", "stV");
  private final Set<String> flowTags = ImmutableSet.of("fTag");

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
  private final AuditMessage auditMessage4 = new AuditMessage(
    0, stream, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      EMPTY_USER_METADATA,
      ImmutableMap.of(MetadataScope.USER, new Metadata(streamProperties, EMPTY_TAGS)),
      EMPTY_USER_METADATA
    )
  );
  private final AuditMessage auditMessage5 = new AuditMessage(
    0, stream, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      ImmutableMap.of(MetadataScope.USER, new Metadata(streamProperties, EMPTY_TAGS)),
      EMPTY_USER_METADATA, EMPTY_USER_METADATA
    )
  );
  private final AuditMessage auditMessage6 = new AuditMessage(
    0, stream, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      ImmutableMap.of(MetadataScope.USER, new Metadata(streamProperties, EMPTY_TAGS)),
      ImmutableMap.of(MetadataScope.USER, new Metadata(updatedStreamProperties, EMPTY_TAGS)),
      ImmutableMap.of(MetadataScope.USER, new Metadata(streamProperties, EMPTY_TAGS))
    )
  );
  private final AuditMessage auditMessage7 = new AuditMessage(
    0, flow, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      EMPTY_USER_METADATA,
      ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, flowTags)),
      EMPTY_USER_METADATA
    )
  );
  private final AuditMessage auditMessage8 = new AuditMessage(
    0, flow, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, flowTags)),
      EMPTY_USER_METADATA,
      ImmutableMap.of(MetadataScope.USER, new Metadata(EMPTY_PROPERTIES, flowTags))
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
  private final AuditMessage auditMessage10 = new AuditMessage(
    0, stream, "", AuditType.METADATA_CHANGE,
    new MetadataPayload(
      ImmutableMap.of(MetadataScope.USER, new Metadata(updatedStreamProperties, EMPTY_TAGS)),
      EMPTY_USER_METADATA,
      ImmutableMap.of(MetadataScope.USER, new Metadata(updatedStreamProperties, EMPTY_TAGS))
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
    auditMessage1, auditMessage2, auditMessage3, auditMessage4, auditMessage5, auditMessage6, auditMessage7,
    auditMessage8, auditMessage9, auditMessage10, auditMessage11
  );

  private static CConfiguration cConf;
  private static TransactionManager txManager;
  private static DefaultMetadataStore store;
  private static InMemoryAuditPublisher auditPublisher;

  @BeforeClass
  public static void setup() {
    Injector injector = Guice.createInjector(
      new ConfigModule(),
      Modules.override(
        new DataSetsModules().getInMemoryModules()).with(new AbstractModule() {
        @Override
        protected void configure() {
          // Need the distributed metadata store.
          bind(MetadataStore.class).to(DefaultMetadataStore.class);
        }
      }),
      new LocationRuntimeModule().getInMemoryModules(),
      new TransactionInMemoryModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new NamespaceClientRuntimeModule().getInMemoryModules(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule(),
      new AuditModule().getInMemoryModules()
    );
    cConf = injector.getInstance(CConfiguration.class);
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    store = injector.getInstance(DefaultMetadataStore.class);
    auditPublisher = injector.getInstance(InMemoryAuditPublisher.class);
  }

  @Before
  public void clearAudit() {
    auditPublisher.popMessages();
  }

  @After
  public void cleanupTest() throws Exception {
    store.deleteDatasets();
  }

  @Test
  public void testPublishing() {
    generateMetadataUpdates();

    // Audit messages for metadata changes
    List<AuditMessage> actualAuditMessages = new ArrayList<>();
    String systemNs = NamespaceId.SYSTEM.getNamespace();
    for (AuditMessage auditMessage : auditPublisher.popMessages()) {
      // Ignore system audit messages
      if (auditMessage.getType() == AuditType.METADATA_CHANGE) {
        if (!auditMessage.getEntity().containsKey(MetadataEntity.NAMESPACE) || !auditMessage.getEntity().getValue
          (MetadataEntity.NAMESPACE).equalsIgnoreCase(systemNs)) {
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

  @Test
  public void testSearchWeight() throws Exception {
    ProgramId flow1 = new ProgramId("ns1", "app1", ProgramType.FLOW, "flow1");
    StreamId stream1 = new StreamId("ns1", "s1");
    DatasetId dataset1 = new DatasetId("ns1", "ds1");

    // Add metadata
    String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";
    Map<String, String> flowUserProps = ImmutableMap.of("key1", "value1",
                                                        "key2", "value2",
                                                        "multiword", multiWordValue);
    Map<String, String> flowSysProps = ImmutableMap.of("sysKey1", "sysValue1");
    Set<String> flowUserTags = ImmutableSet.of("tag1", "tag2");
    Set<String> streamUserTags = ImmutableSet.of("tag3", "tag4");
    Set<String> flowSysTags = ImmutableSet.of("sysTag1");
    store.setProperties(MetadataScope.USER, flow1.toMetadataEntity(), flowUserProps);
    store.setProperties(MetadataScope.SYSTEM, flow1.toMetadataEntity(), flowSysProps);
    store.addTags(MetadataScope.USER, flow1.toMetadataEntity(), flowUserTags);
    store.addTags(MetadataScope.SYSTEM, flow1.toMetadataEntity(), flowSysTags);
    store.addTags(MetadataScope.USER, stream1.toMetadataEntity(), streamUserTags);
    store.removeTags(MetadataScope.USER, stream1.toMetadataEntity(), streamUserTags);
    store.setProperties(MetadataScope.USER, stream1.toMetadataEntity(), flowUserProps);
    store.removeProperties(MetadataScope.USER, stream1.toMetadataEntity(),
                           ImmutableSet.of("key1", "key2", "multiword"));

    Map<String, String> streamUserProps = ImmutableMap.of("sKey1", "sValue1 sValue2",
                                                          "Key1", "Value1");
    store.setProperties(MetadataScope.USER, stream1.toMetadataEntity(), streamUserProps);

    Map<String, String> datasetUserProps = ImmutableMap.of("sKey1", "sValuee1 sValuee2");
    store.setProperties(MetadataScope.USER, dataset1.toMetadataEntity(), datasetUserProps);

    // Test score and metadata match
    MetadataSearchResponseV2 response = search("ns1", "value1 multiword:av2");
    Assert.assertEquals(2, response.getTotal());
    List<MetadataSearchResultRecordV2> actual = Lists.newArrayList(response.getResults());

    Map<MetadataScope, Metadata> expectedFlowMetadata =
      ImmutableMap.of(MetadataScope.USER, new Metadata(flowUserProps, flowUserTags),
                      MetadataScope.SYSTEM, new Metadata(flowSysProps, flowSysTags));
    Map<MetadataScope, Metadata> expectedStreamMetadata =
      ImmutableMap.of(MetadataScope.USER, new Metadata(streamUserProps, Collections.emptySet()));
    Map<MetadataScope, Metadata> expectedDatasetMetadata =
      ImmutableMap.of(MetadataScope.USER, new Metadata(datasetUserProps, Collections.emptySet()));
    List<MetadataSearchResultRecordV2> expected =
      Lists.newArrayList(
        new MetadataSearchResultRecordV2(flow1,
                                         expectedFlowMetadata),
        new MetadataSearchResultRecordV2(stream1,
                                         expectedStreamMetadata)
      );
    Assert.assertEquals(expected, actual);

    response = search("ns1", "value1 sValue*");
    Assert.assertEquals(3, response.getTotal());
    actual = Lists.newArrayList(response.getResults());
    expected = Lists.newArrayList(
      new MetadataSearchResultRecordV2(stream1,
                                       expectedStreamMetadata),
      new MetadataSearchResultRecordV2(dataset1,
                                       expectedDatasetMetadata),
      new MetadataSearchResultRecordV2(flow1,
                                       expectedFlowMetadata)
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

    store.setProperty(MetadataScope.USER, ns1app1, "k1", "v1");
    store.addTags(MetadataScope.USER, ns1app1, Collections.singleton("v1"));
    Metadata meta = new Metadata(Collections.singletonMap("k1", "v1"), Collections.singleton("v1"));
    MetadataSearchResultRecordV2 ns1app1Record =
      new MetadataSearchResultRecordV2(ns1app1, Collections.singletonMap(MetadataScope.USER, meta));

    store.setProperty(MetadataScope.USER, ns1app2, "k1", "v1");
    store.setProperty(MetadataScope.USER, ns1app2, "k2", "v2");
    meta = new Metadata(ImmutableMap.of("k1", "v1", "k2", "v2"), Collections.emptySet());
    MetadataSearchResultRecordV2 ns1app2Record =
      new MetadataSearchResultRecordV2(ns1app2, Collections.singletonMap(MetadataScope.USER, meta));

    store.setProperty(MetadataScope.USER, ns1app3, "k1", "v1");
    store.setProperty(MetadataScope.USER, ns1app3, "k3", "v3");
    meta = new Metadata(ImmutableMap.of("k1", "v1", "k3", "v3"), Collections.emptySet());
    MetadataSearchResultRecordV2 ns1app3Record =
      new MetadataSearchResultRecordV2(ns1app3, Collections.singletonMap(MetadataScope.USER, meta));

    store.setProperty(MetadataScope.USER, ns2app1, "k1", "v1");
    store.setProperty(MetadataScope.USER, ns2app1, "k2", "v2");
    meta = new Metadata(ImmutableMap.of("k1", "v1", "k2", "v2"), Collections.emptySet());
    MetadataSearchResultRecordV2 ns2app1Record =
      new MetadataSearchResultRecordV2(ns2app1, Collections.singletonMap(MetadataScope.USER, meta));

    store.setProperty(MetadataScope.USER, ns2app2, "k1", "v1");
    store.addTags(MetadataScope.USER, ns2app2, ImmutableSet.of("v2", "v3"));
    meta = new Metadata(ImmutableMap.of("k1", "v1"), ImmutableSet.of("v2", "v3"));
    MetadataSearchResultRecordV2 ns2app2Record =
      new MetadataSearchResultRecordV2(ns2app2, Collections.singletonMap(MetadataScope.USER, meta));

    // everything should match 'v1'
    SearchRequest request = new SearchRequest(null, "v1", EnumSet.allOf(EntityTypeSimpleName.class), SortInfo.DEFAULT,
                                              0, 10, 0, null, false, EnumSet.allOf(EntityScope.class));
    MetadataSearchResponseV2 results = store.search(request);
    Set<MetadataSearchResultRecordV2> expected = new HashSet<>();
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
  public void testSubEntityDeletion() {
    // test for tags
    MetadataEntity dsEntity = NamespaceId.DEFAULT.dataset("myDs").toMetadataEntity();
    MetadataEntity fieldEntity = MetadataEntity.builder(dsEntity).appendAsType("field", "empName").build();
    MetadataEntity deepEntity = MetadataEntity.builder(fieldEntity).appendAsType("deep", "anotherLevel").build();

    store.addTags(MetadataScope.USER, dsEntity, ImmutableSet.of("someTag"));
    store.addTags(MetadataScope.USER, fieldEntity, ImmutableSet.of("anotherTag"));
    store.addTags(MetadataScope.USER, fieldEntity, ImmutableSet.of("moreTags"));
    store.addTags(MetadataScope.USER, deepEntity, ImmutableSet.of("deepTag"));

    store.removeMetadata(dsEntity);

    Assert.assertTrue(store.getTags(dsEntity).isEmpty());
    Assert.assertTrue(store.getTags(fieldEntity).isEmpty());
    Assert.assertTrue(store.getTags(deepEntity).isEmpty());

    // test for key-value properties
    store.setProperties(MetadataScope.USER, dsEntity, ImmutableMap.of("k1", "v1"));
    store.setProperties(MetadataScope.USER, fieldEntity, ImmutableMap.of("k2", "v2"));
    store.setProperties(MetadataScope.USER, fieldEntity, ImmutableMap.of("k3", "v3"));
    store.setProperties(MetadataScope.USER, deepEntity, ImmutableMap.of("k4", "v4"));

    store.removeMetadata(dsEntity);

    Assert.assertTrue(store.getProperties(dsEntity).isEmpty());
    Assert.assertTrue(store.getProperties(fieldEntity).isEmpty());
    Assert.assertTrue(store.getProperties(deepEntity).isEmpty());

    // test for system entities as child
    MetadataEntity appEntity = NamespaceId.DEFAULT.app("app1").toMetadataEntity();
    MetadataEntity progEntity = NamespaceId.DEFAULT.app("app1").program(ProgramType.MAPREDUCE, "mr1")
      .toMetadataEntity();

    store.addTags(MetadataScope.USER, appEntity, ImmutableSet.of("someTag"));
    store.addTags(MetadataScope.USER, progEntity, ImmutableSet.of("anotherTag"));
    store.addTags(MetadataScope.USER, progEntity, ImmutableSet.of("moreTags"));
    store.setProperties(MetadataScope.USER, appEntity, ImmutableMap.of("k1", "v1"));
    store.setProperties(MetadataScope.USER, progEntity, ImmutableMap.of("k2", "v2"));
    store.setProperties(MetadataScope.USER, progEntity, ImmutableMap.of("k3", "v3"));

    // removing metadata for app should not remove metadata for program which is known cdap type
    store.removeMetadata(appEntity);
    Assert.assertTrue(store.getProperties(appEntity).isEmpty());
    Assert.assertTrue(store.getTags(appEntity).isEmpty());
    Assert.assertFalse(store.getProperties(progEntity).isEmpty());
    Assert.assertFalse(store.getTags(progEntity).isEmpty());

    store.removeMetadata(progEntity);
    Assert.assertTrue(store.getProperties(progEntity).isEmpty());
    Assert.assertTrue(store.getTags(progEntity).isEmpty());
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

    Metadata meta = new Metadata(Collections.emptyMap(), Collections.singleton("v1"));

    store.addTags(MetadataScope.USER, ns1app1, Collections.singleton("v1"));
    store.addTags(MetadataScope.USER, ns1app2, Collections.singleton("v1"));
    store.addTags(MetadataScope.USER, ns1app3, Collections.singleton("v1"));
    store.addTags(MetadataScope.USER, ns2app1, Collections.singleton("v1"));
    store.addTags(MetadataScope.USER, ns2app2, Collections.singleton("v1"));

    // first get everything
    SearchRequest request = new SearchRequest(null, "*", EnumSet.allOf(EntityTypeSimpleName.class), SortInfo.DEFAULT,
                                              0, 10, 0, null, false, EnumSet.allOf(EntityScope.class));
    MetadataSearchResponseV2 results = store.search(request);
    Assert.assertEquals(5, results.getResults().size());

    Iterator<MetadataSearchResultRecordV2> resultIter = results.getResults().iterator();

    MetadataSearchResultRecordV2 result1 = resultIter.next();
    MetadataSearchResultRecordV2 result2 = resultIter.next();
    MetadataSearchResultRecordV2 result3 = resultIter.next();
    MetadataSearchResultRecordV2 result4 = resultIter.next();
    MetadataSearchResultRecordV2 result5 = resultIter.next();

    // get 4 results (guaranteed to have at least one from each namespace), offset 1
    request = new SearchRequest(null, "*", EnumSet.allOf(EntityTypeSimpleName.class), SortInfo.DEFAULT,
                                1, 4, 0, null, false, EnumSet.allOf(EntityScope.class));
    results = store.search(request);

    List<MetadataSearchResultRecordV2> expected = new ArrayList<>();
    expected.add(result2);
    expected.add(result3);
    expected.add(result4);
    expected.add(result5);
    List<MetadataSearchResultRecordV2> actual = new ArrayList<>(results.getResults());
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
    MetadataEntity flow = nsId.app("app").flow("flow").toMetadataEntity();
    MetadataEntity stream = nsId.stream("stream").toMetadataEntity();
    MetadataEntity dataset = nsId.dataset("dataset").toMetadataEntity();
    // add a dummy entity which starts with _ to test that it doesnt show up see: CDAP-7910
    MetadataEntity trackerDataset = nsId.dataset("_auditLog").toMetadataEntity();

    store.addTags(MetadataScope.USER, flow, ImmutableSet.of("tag", "tag1"));
    store.addTags(MetadataScope.USER, stream, ImmutableSet.of("tag2", "tag3 tag4"));
    store.addTags(MetadataScope.USER, dataset, ImmutableSet.of("tag5 tag6", "tag7 tag8"));
    // add bunch of tags to ensure higher weight to this entity in search result
    store.addTags(MetadataScope.USER, trackerDataset, ImmutableSet.of("tag9", "tag10", "tag11", "tag12", "tag13"));

    MetadataSearchResultRecordV2 flowSearchResult = new MetadataSearchResultRecordV2(flow);
    MetadataSearchResultRecordV2 streamSearchResult = new MetadataSearchResultRecordV2(stream);
    MetadataSearchResultRecordV2 datasetSearchResult = new MetadataSearchResultRecordV2(dataset);
    MetadataSearchResultRecordV2 trackerDatasetSearchResult = new MetadataSearchResultRecordV2(trackerDataset);

    // relevance order for searchQuery "tag*" is trackerDataset, dataset, stream, flow
    // (this depends on how many tags got matched with the search query)
    // trackerDataset entity should not be part
    MetadataSearchResponseV2 response = search(nsId.getNamespace(), "tag*", 0, Integer.MAX_VALUE, 1);
    Assert.assertEquals(3, response.getTotal());
    Assert.assertEquals(
      ImmutableList.of(datasetSearchResult, streamSearchResult, flowSearchResult),
      ImmutableList.copyOf(stripMetadata(response.getResults()))
    );

    // trackerDataset entity should be be part since showHidden is true
    response = search(nsId.getNamespace(), "tag*", 0, Integer.MAX_VALUE, 1, true);
    Assert.assertEquals(4, response.getTotal());
    Assert.assertEquals(
      ImmutableList.of(trackerDatasetSearchResult, datasetSearchResult, streamSearchResult, flowSearchResult),
      ImmutableList.copyOf(stripMetadata(response.getResults()))
    );

    response = search(nsId.getNamespace(), "tag*", 0, 2, 1);
    Assert.assertEquals(3, response.getTotal());
    Assert.assertEquals(
      ImmutableList.of(datasetSearchResult, streamSearchResult),
      ImmutableList.copyOf(stripMetadata(response.getResults()))
    );

    // skipping trackerDataset should not affect the offset
    response = search(nsId.getNamespace(), "tag*", 1, 2, 1);
    Assert.assertEquals(3, response.getTotal());
    Assert.assertEquals(
      ImmutableList.of(streamSearchResult, flowSearchResult),
      ImmutableList.copyOf(stripMetadata(response.getResults()))
    );

    // if showHidden is true trackerDataset should affect the offset
    response = search(nsId.getNamespace(), "tag*", 1, 3, 1, true);
    Assert.assertEquals(4, response.getTotal());
    Assert.assertEquals(
      ImmutableList.of(datasetSearchResult, streamSearchResult, flowSearchResult),
      ImmutableList.copyOf(stripMetadata(response.getResults()))
    );

    response = search(nsId.getNamespace(), "tag*", 2, 2, 1);
    Assert.assertEquals(3, response.getTotal());
    Assert.assertEquals(
      ImmutableList.of(flowSearchResult),
      ImmutableList.copyOf(stripMetadata(response.getResults()))
    );

    response = search(nsId.getNamespace(), "tag*", 4, 2, 1);
    Assert.assertEquals(3, response.getTotal());
    Assert.assertEquals(
      ImmutableList.<MetadataSearchResultRecordV2>of(),
      ImmutableList.copyOf(stripMetadata(response.getResults()))
    );

    // ensure overflow bug is squashed (JIRA: CDAP-8133)
    response = search(nsId.getNamespace(), "tag*", 1, Integer.MAX_VALUE, 0);
    Assert.assertEquals(3, response.getTotal());
    Assert.assertEquals(
      ImmutableList.of(streamSearchResult, flowSearchResult),
      ImmutableList.copyOf(stripMetadata(response.getResults()))
    );
  }

  @AfterClass
  public static void teardown() {
    txManager.stopAndWait();
  }

  private MetadataSearchResponseV2 search(String ns, String searchQuery) throws BadRequestException {
    return search(ns, searchQuery, 0, Integer.MAX_VALUE, 0);
  }

  private MetadataSearchResponseV2 search(String ns, String searchQuery,
                                          int offset, int limit, int numCursors) {
    return search(ns, searchQuery, offset, limit, numCursors, false);
  }

  private MetadataSearchResponseV2 search(String ns, String searchQuery,
                                          int offset, int limit, int numCursors, boolean showHidden) {
    return search(ns, searchQuery, offset, limit, numCursors, showHidden, SortInfo.DEFAULT);
  }

  private MetadataSearchResponseV2 search(String ns, String searchQuery,
                                          int offset, int limit, int numCursors, boolean showHidden,
                                          SortInfo sortInfo) {
    SearchRequest request =
      new SearchRequest(new NamespaceId(ns), searchQuery, EnumSet.allOf(EntityTypeSimpleName.class), sortInfo, offset,
                        limit, numCursors, "", showHidden, EnumSet.allOf(EntityScope.class));
    return store.search(request);
  }

  private void generateMetadataUpdates() {
    store.addTags(MetadataScope.USER, dataset.toMetadataEntity(), datasetTags);
    store.setProperties(MetadataScope.USER, app.toMetadataEntity(), appProperties);
    store.addTags(MetadataScope.USER, app.toMetadataEntity(), appTags);
    store.setProperties(MetadataScope.USER, stream.toMetadataEntity(), streamProperties);
    store.setProperties(MetadataScope.USER, stream.toMetadataEntity(), streamProperties);
    store.setProperties(MetadataScope.USER, stream.toMetadataEntity(), updatedStreamProperties);
    store.addTags(MetadataScope.USER, flow.toMetadataEntity(), flowTags);
    store.removeTags(MetadataScope.USER, flow.toMetadataEntity());
    store.removeTags(MetadataScope.USER, dataset.toMetadataEntity(), datasetTags);
    store.removeProperties(MetadataScope.USER, stream.toMetadataEntity());
    store.removeMetadata(MetadataScope.USER, app.toMetadataEntity());
  }

  private Set<MetadataSearchResultRecordV2> stripMetadata(Set<MetadataSearchResultRecordV2> searchResultsWithMetadata) {
    Set<MetadataSearchResultRecordV2> metadataStrippedResults = new LinkedHashSet<>(searchResultsWithMetadata.size());
    for (MetadataSearchResultRecordV2 searchResultWithMetadata : searchResultsWithMetadata) {
      metadataStrippedResults.add(new MetadataSearchResultRecordV2(searchResultWithMetadata.getEntityId()));
    }
    return metadataStrippedResults;
  }
}
