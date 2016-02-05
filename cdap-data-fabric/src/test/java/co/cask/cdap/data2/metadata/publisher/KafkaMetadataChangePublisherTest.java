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

package co.cask.cdap.data2.metadata.publisher;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.metadata.store.DefaultMetadataStore;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.kafka.KafkaTester;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.metadata.MetadataChangeRecord;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.cdap.proto.metadata.MetadataScope;
import co.cask.tephra.runtime.TransactionInMemoryModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.util.Modules;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Tests for {@link KafkaMetadataChangePublisher}.
 */
public class KafkaMetadataChangePublisherTest {
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
      new NamespaceClientRuntimeModule().getInMemoryModules()
    ),
    1,
    Constants.Metadata.UPDATES_KAFKA_BROKER_LIST
  );
  private static MetadataChangePublisher publisher;

  @Before
  public void setup() throws IOException {
    publisher = KAFKA_TESTER.getInjector().getInstance(MetadataChangePublisher.class);
  }

  @Test
  public void testPublish() throws InterruptedException {
    List<MetadataChangeRecord> metadataChangeRecords = generateMetadataChanges();
    for (MetadataChangeRecord metadataChangeRecord : metadataChangeRecords) {
      publisher.publish(metadataChangeRecord);
    }
    String topic = KAFKA_TESTER.getCConf().get(Constants.Metadata.UPDATES_KAFKA_TOPIC);
    Type metadataChangeRecordType = new TypeToken<MetadataChangeRecord>() { }.getType();
    Assert.assertEquals(metadataChangeRecords, KAFKA_TESTER.getPublishedMessages(topic, metadataChangeRecords.size(),
                                                                                 metadataChangeRecordType, GSON));
  }

  private List<MetadataChangeRecord> generateMetadataChanges() {
    long currentTime = System.currentTimeMillis();
    ImmutableList.Builder<MetadataChangeRecord> changesBuilder = ImmutableList.builder();
    Id.DatasetInstance dataset = Id.DatasetInstance.from("ns1", "ds1");
    // Initial state: empty
    MetadataRecord previous = new MetadataRecord(dataset, MetadataScope.USER, ImmutableMap.<String, String>of(),
                                                 ImmutableSet.<String>of());
    // Change 1: add one property and 1 tag
    MetadataChangeRecord.MetadataDiffRecord initialAddition = new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(dataset, MetadataScope.USER, ImmutableMap.of("key1", "value1"), ImmutableSet.of("tag1")),
      new MetadataRecord(dataset, MetadataScope.USER, ImmutableMap.<String, String>of(), ImmutableSet.<String>of())
    );
    long updateTime = currentTime - 1000;
    changesBuilder.add(new MetadataChangeRecord(previous, initialAddition, updateTime));
    // Metadata after initial addition
    previous = new MetadataRecord(dataset, MetadataScope.USER, ImmutableMap.of("key1", "value1"),
                                  ImmutableSet.of("tag1"));
    // Change 1: update a property - translates to one addition and one deletion
    MetadataChangeRecord.MetadataDiffRecord propUpdated = new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(dataset, MetadataScope.USER, ImmutableMap.of("key1", "v1"), ImmutableSet.<String>of()),
      new MetadataRecord(dataset, MetadataScope.USER, ImmutableMap.of("key1", "value1"), ImmutableSet.<String>of())
    );
    updateTime = currentTime - 800;
    changesBuilder.add(new MetadataChangeRecord(previous, propUpdated, updateTime));
    // Metadata after property update
    previous = new MetadataRecord(dataset, MetadataScope.USER, ImmutableMap.of("key1", "v1"), ImmutableSet.of("tag1"));
    // Change 2: add a new property - translates to one addition
    MetadataChangeRecord.MetadataDiffRecord propAdded = new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(dataset, MetadataScope.USER, ImmutableMap.of("key2", "value2"), ImmutableSet.<String>of()),
      new MetadataRecord(dataset, MetadataScope.USER, ImmutableMap.<String, String>of(), ImmutableSet.<String>of())
    );
    updateTime = currentTime - 600;
    changesBuilder.add(new MetadataChangeRecord(previous, propAdded, updateTime));
    // Metadata after property addition
    previous = new MetadataRecord(dataset, MetadataScope.USER, ImmutableMap.of("key1", "v1", "key2", "value2"),
                                  ImmutableSet.of("tag1"));
    // Change 3: remove a property - translates to 1 deletion
    MetadataChangeRecord.MetadataDiffRecord propRemoved = new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(dataset, MetadataScope.USER, ImmutableMap.<String, String>of(), ImmutableSet.<String>of()),
      new MetadataRecord(dataset, MetadataScope.USER, ImmutableMap.of("key1", "v1"), ImmutableSet.<String>of())
    );
    updateTime = currentTime - 400;
    changesBuilder.add(new MetadataChangeRecord(previous, propRemoved, updateTime));
    // Metadata after property deletion
    previous = new MetadataRecord(dataset, MetadataScope.USER,
                                  ImmutableMap.of("key2", "value2"), ImmutableSet.of("tag1"));
    // Change 4: remove a tag - translates to 1 deletion
    MetadataChangeRecord.MetadataDiffRecord tagRemoved = new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(dataset, MetadataScope.USER, ImmutableMap.<String, String>of(), ImmutableSet.<String>of()),
      new MetadataRecord(dataset, MetadataScope.USER, ImmutableMap.<String, String>of(), ImmutableSet.of("tag1"))
    );
    updateTime = currentTime - 200;
    changesBuilder.add(new MetadataChangeRecord(previous, tagRemoved, updateTime));
    // Metadata after tag removal
    previous = new MetadataRecord(dataset, MetadataScope.USER, ImmutableMap.of("key2", "value2"),
                                  ImmutableSet.<String>of());
    // Change 5: add a tag - translates to 1 addition
    MetadataChangeRecord.MetadataDiffRecord tagAdded = new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(dataset, MetadataScope.USER, ImmutableMap.<String, String>of(), ImmutableSet.of("tag1")),
      new MetadataRecord(dataset, MetadataScope.USER, ImmutableMap.<String, String>of(), ImmutableSet.<String>of())
    );
    updateTime = currentTime;
    changesBuilder.add(new MetadataChangeRecord(previous, tagAdded, updateTime));
    return changesBuilder.build();
  }
}
