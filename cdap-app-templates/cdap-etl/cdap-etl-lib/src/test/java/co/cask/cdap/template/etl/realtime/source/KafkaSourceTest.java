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

package co.cask.cdap.template.etl.realtime.source;

import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.realtime.SourceState;
import co.cask.cdap.template.etl.common.MockRealtimeContext;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.internal.utils.Networks;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  Unit test for {@link KafkaSource} ETL realtime source class.
 *
 *  The base code is borrowed from {@code cdap-kafka-pack} project.
 * </p>
 */
public class KafkaSourceTest {

  private static ZKClientService zkClient;
  private static KafkaClientService kafkaClient;

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  protected static final int PARTITIONS = 1;

  protected static InMemoryZKServer zkServer;
  protected static EmbeddedKafkaServer kafkaServer;
  protected static int kafkaPort;

  private KafkaSource kafkaSource;

  @BeforeClass
  public static void beforeClass() throws IOException {
    zkServer = InMemoryZKServer.builder().setDataDir(TMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();

    kafkaPort = Networks.getRandomPort();
    kafkaServer = new EmbeddedKafkaServer(generateKafkaConfig(zkServer.getConnectionStr(),
                                                              kafkaPort, TMP_FOLDER.newFolder()));
    kafkaServer.startAndWait();

    zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();

    kafkaClient = new ZKKafkaClientService(zkClient);
    kafkaClient.startAndWait();
  }

  @AfterClass
  public static void afterClass() {
    kafkaServer.stopAndWait();
    zkServer.stopAndWait();
    kafkaClient.stopAndWait();
    zkClient.stopAndWait();
  }

  @Before
  public void beforeTest() {
    kafkaSource = null;
  }

  @After
  public void afterTest() {
    if (kafkaSource != null) {
      kafkaSource.destroy();
    }
  }

  @Test
  public void testKafkaConsumerSimple() throws Exception {
    final String topic = "testKafkaConsumerSimple";

    initializeKafkaSource(topic, PARTITIONS, false);

    // Publish 5 messages to Kafka, the source should consume them
    int msgCount = 5;
    Map<String, String> messages = Maps.newHashMap();
    for (int i = 0; i < msgCount; i++) {
      messages.put(Integer.toString(i), "Message " + i);
    }
    sendMessage(topic, messages);

    TimeUnit.SECONDS.sleep(2);

    verifyEmittedMessages(kafkaSource, msgCount, new SourceState());
  }

  @Test
  public void testSavedSourceState() throws Exception {
    final String topic = "testKafkaSavedSourceState";

    SourceState sourceState = new SourceState();

    initializeKafkaSource(topic, PARTITIONS, false);

    // Publish 5 messages to Kafka, the source should consume them
    int msgCount = 5;
    Map<String, String> messages = Maps.newHashMap();
    for (int i = 0; i < msgCount; i++) {
      messages.put(Integer.toString(i), "Message " + i);
    }
    sendMessage(topic, messages);

    TimeUnit.SECONDS.sleep(2);

    verifyEmittedMessages(kafkaSource, msgCount, sourceState);

    // Do it again but this time the messages will still be 5 instead of 10 from beginning.

    // Reset KafkaSource
    kafkaSource.destroy();
    kafkaSource = null;

    initializeKafkaSource(topic, PARTITIONS, false);

    sendMessage(topic, messages);

    TimeUnit.SECONDS.sleep(2);

    verifyEmittedMessages(kafkaSource, msgCount, sourceState);
  }

  @Test
  public void testStructuredRecord() throws Exception {
    final String topic = "testKafkaStructuredRecord";
    initializeKafkaSource(topic, PARTITIONS, false, Formats.TEXT);

    // Publish 5 messages to Kafka, the source should consume them
    int msgCount = 5;
    Map<String, String> messages = Maps.newHashMap();
    for (int i = 0; i < msgCount; i++) {
      messages.put(Integer.toString(i), "Message " + i);
    }
    sendMessage(topic, messages);

    TimeUnit.SECONDS.sleep(2);

    MockEmitter emitter = new MockEmitter();
    SourceState updatedSourceState = kafkaSource.poll(emitter, new SourceState());

    System.out.println("Message sent out: " + msgCount);
    System.out.println("Message being emitted by Kafka realtime source: " + emitter.getInternalSize());

    Assert.assertTrue(updatedSourceState.getState() != null && !updatedSourceState.getState().isEmpty());
    Assert.assertTrue(emitter.getInternalSize() == msgCount);
    Assert.assertTrue(((String) emitter.entryList.get(0).get("body")).contains("Message"));
  }

  private void initializeKafkaSource(String topic, int partitions, boolean preferZK) throws Exception {
    initializeKafkaSource(topic, partitions, preferZK, null);
  }

  private void initializeKafkaSource(String topic, int partitions, boolean preferZK, String format) throws Exception {
    String zk = null;
    String brokerList = null;
    if (!supportBrokerList() || preferZK) {
      zk = zkServer.getConnectionStr();
    } else {
      brokerList = "localhost:" + kafkaPort;
    }
    KafkaSource.KafkaPluginConfig config = new KafkaSource.KafkaPluginConfig(zk, brokerList, partitions,
                                                                             topic, null, format, null);

    kafkaSource = new KafkaSource(config);

    kafkaSource.initialize(new MockRealtimeContext());
  }

  // Helper method to verify
  private void verifyEmittedMessages(KafkaSource source, int msgCount, SourceState sourceState)  {
    MockEmitter emitter = new MockEmitter();
    SourceState updatedSourceState = source.poll(emitter, sourceState);

    System.out.println("Message sent out: " + msgCount);
    System.out.println("Message being emitted by Kafka realtime source: " + emitter.getInternalSize());

    Assert.assertTrue(updatedSourceState.getState() != null && !updatedSourceState.getState().isEmpty());
    Assert.assertTrue(emitter.getInternalSize() == msgCount);
  }

  protected void sendMessage(String topic, Map<String, String> messages) {
    // Publish a message to Kafka, the flow should consume it
    KafkaPublisher publisher = kafkaClient.getPublisher(KafkaPublisher.Ack.ALL_RECEIVED, Compression.NONE);

    // If publish failed, retry up to 20 times, with 100ms delay between each retry
    // This is because leader election in Kafka 08 takes time when a topic is being created upon publish request.
    int count = 0;
    do {
      KafkaPublisher.Preparer preparer = publisher.prepare(topic);
      for (Map.Entry<String, String> entry : messages.entrySet()) {
        preparer.add(Charsets.UTF_8.encode(entry.getValue()), entry.getKey());
      }
      try {
        preparer.send().get();
        break;
      } catch (Exception e) {
        // Backoff if send failed.
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      }
    } while (count++ < 20);
  }

  protected boolean supportBrokerList() {
    return true;
  }

  private static Properties generateKafkaConfig(String zkConnectStr, int port, File logDir) {
    // Note: the log size properties below have been set so that we can have log rollovers
    // and log deletions in a minute.
    Properties prop = new Properties();
    prop.setProperty("log.dir", logDir.getAbsolutePath());
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("broker.id", "1");
    prop.setProperty("socket.send.buffer.bytes", "1048576");
    prop.setProperty("socket.receive.buffer.bytes", "1048576");
    prop.setProperty("socket.request.max.bytes", "104857600");
    prop.setProperty("num.partitions", Integer.toString(PARTITIONS));
    prop.setProperty("log.retention.hours", "24");
    prop.setProperty("log.flush.interval.messages", "10");
    prop.setProperty("log.flush.interval.ms", "1000");
    prop.setProperty("log.segment.bytes", "100");
    prop.setProperty("zookeeper.connect", zkConnectStr);
    prop.setProperty("zookeeper.connection.timeout.ms", "1000000");
    prop.setProperty("default.replication.factor", "1");
    prop.setProperty("log.retention.bytes", "1000");
    prop.setProperty("log.retention.check.interval.ms", "60000");

    return prop;
  }

  /**
   * Helper class to emit message to next stage
   */
  private static class MockEmitter implements Emitter<StructuredRecord> {
    private final List<StructuredRecord> entryList = Lists.newArrayList();

    @Override
    public void emit(StructuredRecord value) {
      entryList.add(value);
    }

    public int getInternalSize() {
      return entryList.size();
    }

    public void reset() {
      entryList.clear();
    }
  }

}
