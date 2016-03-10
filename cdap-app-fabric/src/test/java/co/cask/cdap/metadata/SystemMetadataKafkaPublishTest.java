/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.AllProgramsApp;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.metadata.store.DefaultMetadataStore;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.internal.AppFabricClient;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.kafka.KafkaTester;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.metadata.MetadataChangeRecord;
import co.cask.tephra.runtime.TransactionInMemoryModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.util.Modules;
import org.junit.ClassRule;
import org.junit.Test;

import java.lang.reflect.Type;

/**
 * Tests for verifying that system metadata gets published to Kafka when publishing is enabled
 */
public class SystemMetadataKafkaPublishTest {
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

  @Test
  public void testPublishing() throws Exception {
    CConfiguration cConf = KAFKA_TESTER.getCConf();
    AppFabricTestHelper.deployApplication(AllProgramsApp.class, cConf);
    String topic = cConf.get(Constants.Metadata.UPDATES_KAFKA_TOPIC);
    Type metadataChangeRecordType = new TypeToken<MetadataChangeRecord>() { }.getType();
    // Expect 17 messages to be generated for system metadata additions:
    // 1 = for adding tags to artifact
    // 2 = for adding properties and tags to app
    // 6 = 1 each for adding properties to flow, mr, service, spark, workflow, worker
    // 3 = for adding properties, tags and schema for stream
    // 2 = for adding properties and tags to kvt dataset
    // 3 = for adding properties, tags and schema to dsWithSchema dataset
    KAFKA_TESTER.getPublishedMessages(topic, 17, metadataChangeRecordType, GSON);
    AppFabricClient appFabricClient = AppFabricTestHelper.getInjector(cConf).getInstance(AppFabricClient.class);
    appFabricClient.reset();
    // Expect 24 more messages to be generated for metadata deletions:
    // 22 = 2 (1 for USER scope and 1 for SYSTEM scope) each for artifact, app, flow, mr, service, spark, workflow,
    // worker, stream, kvt, dsWithSchema
    // 2 extra for the stream because during namespace delete, stream delete gets called twice - once via
    // StreamAdmin.removeAllInNamespace and once via Store.removeAll
    KAFKA_TESTER.getPublishedMessages(topic, 24, metadataChangeRecordType, GSON, 17);
  }
}
