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

package co.cask.cdap.data2.metadata.publisher;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.KafkaConstants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.metadata.service.BusinessMetadataStore;
import co.cask.cdap.data2.metadata.service.DefaultBusinessMetadataStore;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.metadata.MetadataChangeRecord;
import co.cask.tephra.runtime.TransactionInMemoryModule;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
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
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Base class for metadata tests that use embedded Kafka to publish and verify change notifications.
 */
public class MetadataKafkaTestBase {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec())
    .create();
  private static InMemoryZKServer zkServer;
  private static EmbeddedKafkaServer kafkaServer;
  private static ZKClientService zkClient;

  protected static KafkaClientService kafkaClient;
  protected static CConfiguration cConf;
  protected static Injector injector;

  @BeforeClass
  public static void setup() throws IOException {
    int kafkaPort = Networks.getRandomPort();
    Preconditions.checkState(kafkaPort > 0, "Failed to get random port.");
    int zkServerPort = Networks.getRandomPort();
    zkServer = InMemoryZKServer.builder().setDataDir(TMP_FOLDER.newFolder()).setPort(zkServerPort).build();
    zkServer.startAndWait();
    kafkaServer = new EmbeddedKafkaServer(generateKafkaConfig(kafkaPort));
    kafkaServer.startAndWait();
    cConf = CConfiguration.create();
    cConf.set(Constants.Metadata.UPDATES_PUBLISH_ENABLED, "true");
    cConf.set(Constants.Metadata.UPDATES_KAFKA_BROKER_LIST,
              InetAddress.getLoopbackAddress().getHostAddress() + ":" + kafkaPort);
    cConf.unset(KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG);
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());
    injector = Guice.createInjector(
      new ConfigModule(cConf),
      new LocationRuntimeModule().getInMemoryModules(),
      new ZKClientModule(),
      new KafkaClientModule(),
      new TransactionInMemoryModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      Modules.override(new DataSetsModules().getInMemoryModules(true)).with(new AbstractModule() {
        @Override
        protected void configure() {
          // Need the distributed metadata store.
          bind(BusinessMetadataStore.class).to(DefaultBusinessMetadataStore.class);
        }
      }),
      new NamespaceClientRuntimeModule().getInMemoryModules()
    );
    zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAndWait();
    kafkaClient = injector.getInstance(KafkaClientService.class);
    kafkaClient.startAndWait();
  }

  @AfterClass
  public static void teardown() {
    kafkaClient.stopAndWait();
    zkClient.stopAndWait();
    kafkaServer.stopAndWait();
    zkServer.stopAndWait();
  }

  protected List<MetadataChangeRecord> getPublishedMetadataChanges(int expectedNumRecords) throws InterruptedException {
    return getPublishedMetadataChanges(expectedNumRecords, 0);
  }

  protected List<MetadataChangeRecord> getPublishedMetadataChanges(int expectedNumRecords,
                                                                   int offset) throws InterruptedException {
    String topic = cConf.get(Constants.Metadata.UPDATES_KAFKA_TOPIC);
    final CountDownLatch latch = new CountDownLatch(expectedNumRecords);
    final CountDownLatch stopLatch = new CountDownLatch(1);
    final List<MetadataChangeRecord> actual = new ArrayList<>(expectedNumRecords);
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
    return actual;
  }

  private static Properties generateKafkaConfig(int port) throws IOException {
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
