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

package co.cask.cdap.logging.pipeline.kafka;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.kafka.KafkaTester;
import co.cask.cdap.logging.appender.kafka.LoggingEventSerializer;
import co.cask.cdap.logging.context.GenericLoggingContext;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.twill.kafka.client.BrokerInfo;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class KafkaPerfTest {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaPerfTest.class);

  private static final int PUBLISH_BATCH_SIZE = 1000;
  private static final int PUBLISH_TIMES = 240;
  private static final int TOTAL_EVENTS_COUNT = PUBLISH_BATCH_SIZE * PUBLISH_TIMES;

  @ClassRule
  public static final KafkaTester KAFKA_TESTER =
    new KafkaTester(ImmutableMap.<String, String>of(), ImmutableList.<Module>of(), 1);

  @Test
  public void publishConsumeTest() throws InterruptedException {
    String topic = "kafkaPerfTest";

    String stackTrace = Joiner.on(", ").join(Thread.currentThread().getStackTrace());
    List<ILoggingEvent> events = new ArrayList<>();
    long timestamp = System.currentTimeMillis();
    while (events.size() < PUBLISH_BATCH_SIZE) {
      events.add(createLoggingEvent("test.logger", Level.INFO, stackTrace, timestamp));
    }

    for (int i = 0; i < PUBLISH_TIMES; i++) {
      publishLog(topic, events);
    }

    BrokerInfo brokerInfo = KAFKA_TESTER.getBrokerService().getLeader(topic, 0);
    if (brokerInfo == null) {
      Assert.fail();
    }

    SimpleConsumer consumer
      = new SimpleConsumer(brokerInfo.getHost(), brokerInfo.getPort(),
                           5000, 210000, "simple-kafka-client");
    int messageCount = 0;
    long nextOffset = 0;
    long now = System.currentTimeMillis();
    while (messageCount < TOTAL_EVENTS_COUNT) {
      FetchRequest req = new FetchRequestBuilder()
        .clientId("kafka-perf").addFetch(topic, 0, nextOffset, 200000).build();
      FetchResponse fetchResponse = consumer.fetch(req);
      if (fetchResponse.hasError()) {
        LOG.warn("Fetch response contains error.");
        Thread.sleep(50);
      }
      int batchSize = 0;
      ByteBufferMessageSet messageSet = fetchResponse.messageSet(topic, 0);
      for (MessageAndOffset messageAndOffset : messageSet) {
        nextOffset = messageAndOffset.offset();
        batchSize++;
      }
      messageCount += batchSize;
      LOG.debug("Total {} messages with average size in bytes: {}", batchSize, messageSet.sizeInBytes() / batchSize);
    }
    LOG.info("{} millis taken to iterate through {} messages", System.currentTimeMillis() - now, TOTAL_EVENTS_COUNT);
  }

  /**
   * Creates a new {@link ILoggingEvent} with the given information.
   */
  private ILoggingEvent createLoggingEvent(String loggerName, Level level, String message, long timestamp) {
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
  private void publishLog(String topic, Iterable<ILoggingEvent> events) {
    publishLog(topic, events, new GenericLoggingContext(NamespaceId.DEFAULT.getNamespace(), "app", "entity"));
  }

  private void publishLog(String topic, Iterable<ILoggingEvent> events, LoggingContext context) {
    KafkaPublisher.Preparer preparer = KAFKA_TESTER.getKafkaClient()
      .getPublisher(KafkaPublisher.Ack.LEADER_RECEIVED, Compression.NONE)
      .prepare(topic);

    LoggingEventSerializer serializer = new LoggingEventSerializer();
    for (ILoggingEvent event : events) {
      byte[] payloadBytes = serializer.toBytes(event, context);
//      LOG.debug("Event size in bytes = {}", payloadBytes.length);
      preparer.add(ByteBuffer.wrap(payloadBytes), context.getLogPartition());
    }
    preparer.send();
  }
}
