/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.data2.audit.InMemoryAuditPublisher;
import co.cask.cdap.data2.audit.payload.builder.MetadataPayloadBuilder;
import co.cask.cdap.kafka.KafkaTester;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.audit.AuditMessage;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedId;
import co.cask.cdap.proto.metadata.Metadata;
import co.cask.cdap.proto.metadata.MetadataChangeRecord;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.runtime.TransactionInMemoryModule;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link MetadataStore}
 */
public class MetadataStoreTest {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec())
    .create();

  @ClassRule
  public static final KafkaTester KAFKA_TESTER = new KafkaTester(
    ImmutableMap.of(Constants.Metadata.UPDATES_PUBLISH_ENABLED, "true"),
    ImmutableList.of(
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
      new AuditModule().getInMemoryModules()
    ),
    1,
    Constants.Metadata.UPDATES_KAFKA_BROKER_LIST
  );

  private static final Type METADATA_CHANGE_RECORD_TYPE = new TypeToken<MetadataChangeRecord>() { }.getType();

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
    new MetadataRecord(dataset, MetadataScope.USER),
    new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(dataset, MetadataScope.USER, ImmutableMap.<String, String>of(), datasetTags),
      new MetadataRecord(dataset, MetadataScope.USER)
    ),
    System.currentTimeMillis()
  );
  private final MetadataChangeRecord change2 = new MetadataChangeRecord(
    new MetadataRecord(app, MetadataScope.USER),
    new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(app, MetadataScope.USER, appProperties, ImmutableSet.<String>of()),
      new MetadataRecord(app, MetadataScope.USER)
    ),
    System.currentTimeMillis()
  );
  private final MetadataChangeRecord change3 = new MetadataChangeRecord(
    new MetadataRecord(app, MetadataScope.USER, appProperties, ImmutableSet.<String>of()),
    new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(app, MetadataScope.USER, ImmutableMap.<String, String>of(), appTags),
      new MetadataRecord(app, MetadataScope.USER)
    ),
    System.currentTimeMillis()
  );
  private final MetadataChangeRecord change4 = new MetadataChangeRecord(
    new MetadataRecord(stream, MetadataScope.USER),
    new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(stream, MetadataScope.USER, streamProperties, ImmutableSet.<String>of()),
      new MetadataRecord(stream, MetadataScope.USER)
    ),
    System.currentTimeMillis()
  );
  private final MetadataChangeRecord change5 = new MetadataChangeRecord(
    new MetadataRecord(stream, MetadataScope.USER, streamProperties, ImmutableSet.<String>of()),
    new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(stream, MetadataScope.USER),
      new MetadataRecord(stream, MetadataScope.USER)
    ),
    System.currentTimeMillis()
  );
  private final MetadataChangeRecord change6 = new MetadataChangeRecord(
    new MetadataRecord(stream, MetadataScope.USER, streamProperties, ImmutableSet.<String>of()),
    new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(stream, MetadataScope.USER, updatedStreamProperties, ImmutableSet.<String>of()),
      new MetadataRecord(stream, MetadataScope.USER, streamProperties, ImmutableSet.<String>of())
    ),
    System.currentTimeMillis()
  );
  private final MetadataChangeRecord change7 = new MetadataChangeRecord(
    new MetadataRecord(flow, MetadataScope.USER),
    new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(flow, MetadataScope.USER, ImmutableMap.<String, String>of(), flowTags),
      new MetadataRecord(flow, MetadataScope.USER)
    ),
    System.currentTimeMillis()
  );
  private final MetadataChangeRecord change8 = new MetadataChangeRecord(
    new MetadataRecord(flow, MetadataScope.USER, ImmutableMap.<String, String>of(), flowTags),
    new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(flow, MetadataScope.USER),
      new MetadataRecord(flow, MetadataScope.USER, ImmutableMap.<String, String>of(), flowTags)
    ),
    System.currentTimeMillis()
  );
  private final MetadataChangeRecord change9 = new MetadataChangeRecord(
    new MetadataRecord(dataset, MetadataScope.USER, ImmutableMap.<String, String>of(), datasetTags),
    new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(dataset, MetadataScope.USER),
      new MetadataRecord(dataset, MetadataScope.USER, ImmutableMap.<String, String>of(), datasetTags)
    ),
    System.currentTimeMillis()
  );
  private final MetadataChangeRecord change10 = new MetadataChangeRecord(
    new MetadataRecord(stream, MetadataScope.USER, updatedStreamProperties, ImmutableSet.<String>of()),
    new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(stream, MetadataScope.USER),
      new MetadataRecord(stream, MetadataScope.USER, updatedStreamProperties, ImmutableSet.<String>of())
    ),
    System.currentTimeMillis()
  );
  private final MetadataChangeRecord change11 = new MetadataChangeRecord(
    new MetadataRecord(app, MetadataScope.USER, appProperties, appTags),
    new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(app, MetadataScope.USER, ImmutableMap.<String, String>of(), ImmutableSet.<String>of()),
      new MetadataRecord(app, MetadataScope.USER, appProperties, appTags)
    ),
    System.currentTimeMillis()
  );
  private final List<MetadataChangeRecord> expectedChanges = ImmutableList.of(
  change1, change2, change3, change4, change5, change6, change7, change8, change9, change10, change11);

  private int kafkaOffset = 0;

  private static TransactionManager txManager;
  private static MetadataStore store;
  private static InMemoryAuditPublisher auditPublisher;

  @BeforeClass
  public static void setup() throws IOException {
    Injector injector = KAFKA_TESTER.getInjector();
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    store = injector.getInstance(MetadataStore.class);
    auditPublisher = injector.getInstance(InMemoryAuditPublisher.class);
  }

  @Before
  public void clearAudit() throws Exception {
    auditPublisher.popMessages();
  }

  @Test
  public void testPublishing() throws InterruptedException {
    generateMetadataUpdates();
    String topic = KAFKA_TESTER.getCConf().get(Constants.Metadata.UPDATES_KAFKA_TOPIC);
    List<MetadataChangeRecord> publishedChanges =
      KAFKA_TESTER.getPublishedMessages(topic, expectedChanges.size(), METADATA_CHANGE_RECORD_TYPE, GSON);
    for (int i = 0; i < expectedChanges.size(); i++) {
      MetadataChangeRecord expected = expectedChanges.get(i);
      MetadataChangeRecord actual = publishedChanges.get(i);
      Assert.assertEquals(expected.getPrevious(), actual.getPrevious());
      Assert.assertEquals(expected.getChanges(), actual.getChanges());
    }
    // note kafka offset
    kafkaOffset += publishedChanges.size();

    // Verify audit publishing for the metadata changes
    Function<MetadataChangeRecord, AuditMessage> metadataChangeRecordToAuditMessage =
      new Function<MetadataChangeRecord, AuditMessage>() {
        @Override
        public AuditMessage apply(MetadataChangeRecord input) {
          MetadataPayloadBuilder builder = new MetadataPayloadBuilder();
          builder.addPrevious(input.getPrevious());
          builder.addAdditions(input.getChanges().getAdditions());
          builder.addDeletions(input.getChanges().getDeletions());
          return new AuditMessage(0, input.getPrevious().getEntityId().toEntityId(), "",
                                  AuditType.METADATA_CHANGE, builder.build());
        }
      };

    // Audit messages for metadata changes
    List<AuditMessage> expectedAuditMessages = Lists.transform(expectedChanges, metadataChangeRecordToAuditMessage);
    List<AuditMessage> actualAuditMessages = new ArrayList<>();
    for (AuditMessage auditMessage : auditPublisher.popMessages()) {
      // Ignore system audit messages
      if (auditMessage.getEntityId() instanceof NamespacedId) {
        String systemNs = NamespaceId.SYSTEM.getNamespace();
        if (!((NamespacedId) auditMessage.getEntityId()).getNamespace().equals(systemNs)) {
          actualAuditMessages.add(auditMessage);
        }
      }
    }
    Assert.assertEquals(expectedAuditMessages, actualAuditMessages);
  }

  @Test
  public void testPublishingDisabled() throws InterruptedException {
    CConfiguration cConf = KAFKA_TESTER.getCConf();
    boolean publishEnabled = cConf.getBoolean(Constants.Metadata.UPDATES_PUBLISH_ENABLED);
    cConf.setBoolean(Constants.Metadata.UPDATES_PUBLISH_ENABLED, false);
    generateMetadataUpdates();
    String topic = cConf.get(Constants.Metadata.UPDATES_KAFKA_TOPIC);

    try {
      List<MetadataChangeRecord> publishedChanges =
        KAFKA_TESTER.getPublishedMessages(topic, expectedChanges.size(), METADATA_CHANGE_RECORD_TYPE,
                                          GSON, kafkaOffset);
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

    // Add metadata
    String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";
    Map<String, String> flowUserProps = ImmutableMap.of("key1", "value1",
                                                                 "key2", "value2",
                                                                 "multiword", multiWordValue);
    Map<String, String> flowSysProps = ImmutableMap.of("sysKey1", "sysValue1");
    Set<String> flowUserTags = ImmutableSet.of("tag1", "tag2");
    Set<String> flowSysTags = ImmutableSet.of("sysTag1");
    store.setProperties(MetadataScope.USER, flow1, flowUserProps);
    store.setProperties(MetadataScope.SYSTEM, flow1, flowSysProps);
    store.addTags(MetadataScope.USER, flow1, flowUserTags.toArray(new String[flowUserTags.size()]));
    store.addTags(MetadataScope.SYSTEM, flow1, flowSysTags.toArray(new String[flowSysTags.size()]));

    Map<String, String> streamUserProps = ImmutableMap.of("sKey1", "sValue1 sValue2",
                                                                   "Key1", "Value1");
    store.setProperties(MetadataScope.USER, stream1, streamUserProps);

    Map<String, String> datasetUserProps = ImmutableMap.of("sKey1", "sValuee1 sValuee2");
    store.setProperties(MetadataScope.USER, dataset1, datasetUserProps);

    // Test score and metadata match
    List<MetadataSearchResultRecord> actual = Lists.newArrayList(store.searchMetadata("ns1", "value1 multiword:av2"));

    Map<MetadataScope, Metadata> expectedFlowMetadata =
      ImmutableMap.of(MetadataScope.USER, new Metadata(flowUserProps, flowUserTags),
                      MetadataScope.SYSTEM, new Metadata(flowSysProps, flowSysTags));
    Map<MetadataScope, Metadata> expectedStreamMetadata =
      ImmutableMap.of(MetadataScope.USER, new Metadata(streamUserProps, Collections.<String>emptySet()));
    Map<MetadataScope, Metadata> expectedDatasetMetadata =
      ImmutableMap.of(MetadataScope.USER, new Metadata(datasetUserProps, Collections.<String>emptySet()));
    List<MetadataSearchResultRecord> expected =
      Lists.newArrayList(
        new MetadataSearchResultRecord(flow1,
                                       expectedFlowMetadata),
        new MetadataSearchResultRecord(stream1,
                                       expectedStreamMetadata)
      );
    Assert.assertEquals(expected, actual);

    actual = Lists.newArrayList(store.searchMetadata("ns1", "value1 sValue*"));
    expected = Lists.newArrayList(
      new MetadataSearchResultRecord(stream1,
                                     expectedStreamMetadata),
      new MetadataSearchResultRecord(dataset1,
                                     expectedDatasetMetadata),
      new MetadataSearchResultRecord(flow1,
                                     expectedFlowMetadata)
    );
    Assert.assertEquals(expected, actual);
  }

  @AfterClass
  public static void teardown() {
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
