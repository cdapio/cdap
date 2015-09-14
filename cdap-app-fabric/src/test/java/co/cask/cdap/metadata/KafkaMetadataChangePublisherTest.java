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

package co.cask.cdap.metadata;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.KafkaConstants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.metadata.MetadataChangeRecord;
import co.cask.cdap.proto.metadata.MetadataRecord;
import co.cask.tephra.runtime.TransactionInMemoryModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.twill.common.Cancellable;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.internal.utils.Networks;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link KafkaMetadataChangePublisher}.
 */
public class KafkaMetadataChangePublisherTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec())
    .create();
  private static CConfiguration cConf;
  private static InMemoryZKServer zkServer;
  private static EmbeddedKafkaServer kafkaServer;
  private static ZKClientService zkClient;
  private static KafkaClientService kafkaClient;
  private static MetadataChangePublisher publisher;

  @BeforeClass
  public static void setup() throws IOException, InterruptedException {
    int zkServerPort = Networks.getRandomPort();
    zkServer = InMemoryZKServer.builder().setDataDir(TMP_FOLDER.newFolder()).setPort(zkServerPort).build();
    zkServer.startAndWait();
    kafkaServer = new EmbeddedKafkaServer(generateKafkaConfig());
    kafkaServer.startAndWait();
    cConf = CConfiguration.create();
    cConf.unset(KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG);
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());
    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new ZKClientModule(),
      new KafkaClientModule(),
      new TransactionInMemoryModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new DataSetsModules().getInMemoryModules(),
      new NamespaceClientRuntimeModule().getInMemoryModules(),
      new MetadataServiceModule().getDistributedModules()
    );
    zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAndWait();
    kafkaClient = injector.getInstance(KafkaClientService.class);
    kafkaClient.startAndWait();
    publisher = injector.getInstance(MetadataChangePublisher.class);
  }

  @Test
  public void testPublish() throws InterruptedException {
    List<MetadataChangeRecord> metadataChangeRecords = generateMetadataChanges();
    for (MetadataChangeRecord metadataChangeRecord : metadataChangeRecords) {
      publisher.publish(metadataChangeRecord);
    }
    assertPublishedMetadata(metadataChangeRecords);
  }

  private List<MetadataChangeRecord> generateMetadataChanges() {
    long currentTime = System.currentTimeMillis();
    ImmutableList.Builder<MetadataChangeRecord> changesBuilder = ImmutableList.builder();
    Id.DatasetInstance dataset = Id.DatasetInstance.from("ns1", "ds1");
    // Initial state: empty
    MetadataRecord previous = new MetadataRecord(dataset, ImmutableMap.<String, String>of(),
                                                 ImmutableSet.<String>of());
    // Change 1: add one property and 1 tag
    MetadataChangeRecord.MetadataDiffRecord initialAddition = new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(dataset, ImmutableMap.of("key1", "value1"), ImmutableSet.of("tag1")),
      new MetadataRecord(dataset, ImmutableMap.<String, String>of(), ImmutableSet.<String>of())
    );
    long updateTime = currentTime - 1000;
    changesBuilder.add(new MetadataChangeRecord(previous, initialAddition, updateTime));
    // Metadata after initial addition
    previous = new MetadataRecord(dataset, ImmutableMap.of("key1", "value1"), ImmutableSet.of("tag1"));
    // Change 1: update a property - translates to one addition and one deletion
    MetadataChangeRecord.MetadataDiffRecord propUpdated = new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(dataset, ImmutableMap.of("key1", "v1"), ImmutableSet.<String>of()),
      new MetadataRecord(dataset, ImmutableMap.of("key1", "value1"), ImmutableSet.<String>of())
    );
    updateTime = currentTime - 800;
    changesBuilder.add(new MetadataChangeRecord(previous, propUpdated, updateTime));
    // Metadata after property update
    previous = new MetadataRecord(dataset, ImmutableMap.of("key1", "v1"), ImmutableSet.of("tag1"));
    // Change 2: add a new property - translates to one addition
    MetadataChangeRecord.MetadataDiffRecord propAdded = new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(dataset, ImmutableMap.of("key2", "value2"), ImmutableSet.<String>of()),
      new MetadataRecord(dataset, ImmutableMap.<String, String>of(), ImmutableSet.<String>of())
    );
    updateTime = currentTime - 600;
    changesBuilder.add(new MetadataChangeRecord(previous, propAdded, updateTime));
    // Metadata after property addition
    previous = new MetadataRecord(dataset, ImmutableMap.of("key1", "v1", "key2", "value2"), ImmutableSet.of("tag1"));
    // Change 3: remove a property - translates to 1 deletion
    MetadataChangeRecord.MetadataDiffRecord propRemoved = new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(dataset, ImmutableMap.<String, String>of(), ImmutableSet.<String>of()),
      new MetadataRecord(dataset, ImmutableMap.of("key1", "v1"), ImmutableSet.<String>of())
    );
    updateTime = currentTime - 400;
    changesBuilder.add(new MetadataChangeRecord(previous, propRemoved, updateTime));
    // Metadata after property deletion
    previous = new MetadataRecord(dataset, ImmutableMap.of("key2", "value2"), ImmutableSet.of("tag1"));
    // Change 4: remove a tag - translates to 1 deletion
    MetadataChangeRecord.MetadataDiffRecord tagRemoved = new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(dataset, ImmutableMap.<String, String>of(), ImmutableSet.<String>of()),
      new MetadataRecord(dataset, ImmutableMap.<String, String>of(), ImmutableSet.of("tag1"))
    );
    updateTime = currentTime - 200;
    changesBuilder.add(new MetadataChangeRecord(previous, tagRemoved, updateTime));
    // Metadata after tag removal
    previous = new MetadataRecord(dataset, ImmutableMap.of("key2", "value2"), ImmutableSet.<String>of());
    // Change 5: add a tag - translates to 1 addition
    MetadataChangeRecord.MetadataDiffRecord tagAdded = new MetadataChangeRecord.MetadataDiffRecord(
      new MetadataRecord(dataset, ImmutableMap.<String, String>of(), ImmutableSet.of("tag1")),
      new MetadataRecord(dataset, ImmutableMap.<String, String>of(), ImmutableSet.<String>of())
    );
    updateTime = currentTime;
    changesBuilder.add(new MetadataChangeRecord(previous, tagAdded, updateTime));
    return changesBuilder.build();
  }

  private void assertPublishedMetadata(List<MetadataChangeRecord> expected) throws InterruptedException {
    String topic = cConf.get(Constants.Metadata.METADATA_UPDATES_KAFKA_TOPIC);
    final CountDownLatch latch = new CountDownLatch(expected.size());
    final CountDownLatch stopLatch = new CountDownLatch(1);
    final List<MetadataChangeRecord> actual = new ArrayList<>(expected.size());
    Cancellable cancellable = kafkaClient.getConsumer().prepare().addFromBeginning(topic, 0).consume(
      new KafkaConsumer.MessageCallback() {
        @Override
        public void onReceived(Iterator<FetchedMessage> messages) {
          while (messages.hasNext()) {
            ByteBuffer payload = messages.next().getPayload();
            MetadataChangeRecord record = GSON.fromJson(Bytes.toString(payload), MetadataChangeRecord.class);
            actual.add(record);
            latch.countDown();
          }
        }

        @Override
        public void finished() {
          stopLatch.countDown();
        }
      }
    );
    Assert.assertTrue(latch.await(15, TimeUnit.SECONDS));
    cancellable.cancel();
    Assert.assertTrue(stopLatch.await(15, TimeUnit.SECONDS));
    Assert.assertEquals(expected, actual);
  }

  @AfterClass
  public static void tearDown() {
    kafkaClient.stopAndWait();
    zkClient.stopAndWait();
    kafkaServer.stopAndWait();
    zkServer.stopAndWait();
  }

  private static Properties generateKafkaConfig() throws IOException {
    int port = Networks.getRandomPort();
    Preconditions.checkState(port > 0, "Failed to get random port.");

    Properties prop = new Properties();
    prop.setProperty("broker.id", "1");
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("num.network.threads", "2");
    prop.setProperty("num.io.threads", "2");
    prop.setProperty("socket.send.buffer.bytes", "1048576");
    prop.setProperty("socket.receive.buffer.bytes", "1048576");
    prop.setProperty("socket.request.max.bytes", "104857600");
    prop.setProperty("log.dir", TMP_FOLDER.newFolder().getAbsolutePath());
    prop.setProperty("num.partitions", "1");
    prop.setProperty("log.flush.interval.messages", "10000");
    prop.setProperty("log.flush.interval.ms", "1000");
    prop.setProperty("log.retention.hours", "1");
    prop.setProperty("log.segment.bytes", "536870912");
    prop.setProperty("log.cleanup.interval.mins", "1");
    prop.setProperty("zookeeper.connect", zkServer.getConnectionStr());
    prop.setProperty("zookeeper.connection.timeout.ms", "1000000");

    return prop;
  }
}
