/*
 * Copyright © 2017 Cask Data, Inc.
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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.logging.context.GenericLoggingContext;
import co.cask.cdap.logging.serialize.LoggingEventSerializer;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Joiner;
import com.google.inject.Guice;
import com.google.inject.Injector;
import kafka.admin.AdminUtils;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.twill.kafka.client.BrokerInfo;
import org.apache.twill.kafka.client.BrokerService;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaPerfTest {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaPerfTest.class);

  private static Injector injector;
  private static ZKClientService zkClient;
  private static BrokerService brokerService;
  private static KafkaClientService kafkaClient;

  private static LoggingEventSerializer serializer;
  private static CConfiguration cConf;

  private static final int PUBLISH_BATCH_SIZE = 1024;
  private static final int PUBLISH_TIMES = 1024 * 1024 / 215;
  private static final int TOTAL_EVENTS_COUNT = PUBLISH_BATCH_SIZE * PUBLISH_TIMES;

  public static void main(String[] args) throws Exception {
    new KafkaPerfTest().testPerf();
  }

  public void testPerf() throws Exception {
    String topic = "kafkaPerfTest";
    init();
    createTopic(topic, 1);
    String stackTrace = Joiner.on(", ").join(Thread.currentThread().getStackTrace());
    List<ILoggingEvent> events = new ArrayList<>();
    long timestamp = System.currentTimeMillis();
    while (events.size() < PUBLISH_BATCH_SIZE) {
      events.add(createLoggingEvent("test.logger", Level.INFO, "Waiting for Kafka server to startup...", timestamp));
    }

    KafkaPublisher publisher = kafkaClient.getPublisher(KafkaPublisher.Ack.LEADER_RECEIVED, Compression.NONE);

    for (int i = 0; i < PUBLISH_TIMES; i++) {
      publishLog(publisher, topic, events);
    }

    BrokerInfo brokerInfo = brokerService.getLeader(topic, 0);
    while (brokerInfo == null) {
      TimeUnit.MILLISECONDS.sleep(500);
      brokerInfo = brokerService.getLeader(topic, 0);
    }
    LOG.info("Broker {}:{}", brokerInfo.getHost(), brokerInfo.getPort());

    SimpleConsumer consumer
      = new SimpleConsumer(brokerInfo.getHost(), brokerInfo.getPort(),
                           5000, 10485760, "simple-kafka-client");
    int messageCount = 0;
    long nextOffset = 0;
    long now = System.currentTimeMillis();
    while (messageCount < TOTAL_EVENTS_COUNT) {
      FetchRequest req = new FetchRequestBuilder()
        .clientId("kafka-perf").addFetch(topic, 0, nextOffset, 10485760).build();
      FetchResponse fetchResponse = consumer.fetch(req);
      if (fetchResponse.hasError()) {
        throw new IllegalStateException("Fetch failed");
      }
      int batchSize = 0;
      ByteBufferMessageSet messageSet = fetchResponse.messageSet(topic, 0);
      for (MessageAndOffset messageAndOffset : messageSet) {
        nextOffset = messageAndOffset.offset();
        serializer.fromBytes(messageAndOffset.message().payload());
        batchSize++;
      }
      messageCount += batchSize;
//      LOG.debug("Total {} messages with average size in bytes: {}", batchSize, messageSet.sizeInBytes() / batchSize);
    }
    LOG.info("{} millis taken to iterate through {} messages", System.currentTimeMillis() - now, TOTAL_EVENTS_COUNT);

    brokerService.stopAndWait();
    kafkaClient.stopAndWait();
    zkClient.stopAndWait();
  }
  
  private static void init() throws Exception {
    cConf = CConfiguration.create();
    injector = Guice.createInjector(
      new ConfigModule(cConf),
      new ZKClientModule(),
      new KafkaClientModule()
    );
    serializer = new LoggingEventSerializer();
    zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAndWait();
    kafkaClient = injector.getInstance(KafkaClientService.class);
    kafkaClient.startAndWait();
    brokerService = injector.getInstance(BrokerService.class);
    brokerService.startAndWait();
    LOG.info("Waiting for Kafka server to startup...");
  }

  /**
   * Creates a new {@link ILoggingEvent} with the given information.
   */
  private static ILoggingEvent createLoggingEvent(String loggerName, Level level, String message, long timestamp) {
    LoggingEvent event = new LoggingEvent();
    event.setLevel(level);
    event.setLoggerName(loggerName);
    event.setMessage(message);
    event.setTimeStamp(timestamp);
    return event;
  }

  /**
   * Publishes multiple log events.
   */
  private static void publishLog(KafkaPublisher publisher, String topic, Iterable<ILoggingEvent> events) {
    publishLog(publisher, topic, events,
               new GenericLoggingContext(NamespaceId.DEFAULT.getNamespace(), "app", "entity"));
  }

  private static void publishLog(KafkaPublisher publisher, String topic,
                                 Iterable<ILoggingEvent> events, LoggingContext context) {
    KafkaPublisher.Preparer preparer = publisher.prepare(topic);

    LoggingEventSerializer serializer = new LoggingEventSerializer();
    for (ILoggingEvent event : events) {
      byte[] payloadBytes = serializer.toBytes(event);
//      LOG.debug("Event size in bytes = {}", payloadBytes.length);
      preparer.add(ByteBuffer.wrap(payloadBytes), context.getLogPartition());
    }
    preparer.send();
  }

  public void createTopic(String topic, int partitions) {
    String zkStr = zkClient.getConnectString() + "/kafka";
    AdminUtils.createTopic(new ZkClient(zkStr, 20000, 2000, ZKStringSerializer$.MODULE$),
                           topic, partitions, 1, new Properties());
  }

}
