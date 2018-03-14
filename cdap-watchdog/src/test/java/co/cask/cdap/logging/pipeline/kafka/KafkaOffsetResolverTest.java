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
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.kafka.KafkaTester;
import co.cask.cdap.logging.context.GenericLoggingContext;
import co.cask.cdap.logging.meta.Checkpoint;
import co.cask.cdap.logging.serialize.LoggingEventSerializer;
import co.cask.cdap.metrics.collect.LocalMetricsCollectionService;
import co.cask.cdap.metrics.store.DefaultMetricStore;
import co.cask.cdap.metrics.store.LocalMetricsDatasetFactory;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import org.apache.tephra.runtime.TransactionModules;
import org.apache.twill.common.Cancellable;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Unit-test for {@link KafkaOffsetResolver}.
 */
public class KafkaOffsetResolverTest {

  private static final long EVENT_DELAY_MILLIS = 60000L;
  private static final Random RANDOM = new Random();


  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @ClassRule
  public static final KafkaTester KAFKA_TESTER =
    new KafkaTester(ImmutableMap.<String, String>of(),
                    ImmutableList.of(
                      new NonCustomLocationUnitTestModule().getModule(),
                      new DataSetsModules().getInMemoryModules(),
                      new TransactionModules().getInMemoryModules(),
                      new SystemDatasetRuntimeModule().getInMemoryModules(),
                      new AuthorizationTestModule(),
                      new AuthorizationEnforcementModule().getInMemoryModules(),
                      new AuthenticationContextModules().getNoOpModule(),
                      new AbstractModule() {
                        @Override
                        protected void configure() {
                          bind(MetricsCollectionService.class).to(LocalMetricsCollectionService.class);
                          bind(MetricDatasetFactory.class).to(LocalMetricsDatasetFactory.class).in(Scopes.SINGLETON);
                          bind(MetricStore.class).to(DefaultMetricStore.class);
                        }
                      }),
                    1);

  @Test
  public void testOutOfOrderEvents() throws Exception {
    String topic = "testOutOfOrderEvents";
    KafkaPipelineConfig config = new KafkaPipelineConfig(topic, Collections.singleton(0), 1024L, EVENT_DELAY_MILLIS,
                                                         1048576, 200L);
    KAFKA_TESTER.createTopic(topic, 1);

    // Publish log messages to Kafka and wait for all messages to be published
    long baseTime = System.currentTimeMillis() - 2 * EVENT_DELAY_MILLIS;
    List<ILoggingEvent> outOfOrderEvents = ImmutableList.of(
      createLoggingEvent("test.logger", Level.INFO, "0", baseTime - 20 * 1000 - EVENT_DELAY_MILLIS),
      createLoggingEvent("test.logger", Level.INFO, "0", baseTime - 20 * 1000 - EVENT_DELAY_MILLIS),
      createLoggingEvent("test.logger", Level.INFO, "1", baseTime - 7 * 1000 - EVENT_DELAY_MILLIS),
      createLoggingEvent("test.logger", Level.INFO, "2", baseTime - 9 * 100),
      createLoggingEvent("test.logger", Level.INFO, "3", baseTime - 500),
      createLoggingEvent("test.logger", Level.INFO, "1", baseTime - 9 * 1000),
      createLoggingEvent("test.logger", Level.INFO, "1", baseTime - 9 * 1000 + EVENT_DELAY_MILLIS / 2),
      createLoggingEvent("test.logger", Level.INFO, "1", baseTime - 9 * 1000),
      createLoggingEvent("test.logger", Level.INFO, "1", baseTime - 9 * 1000 - EVENT_DELAY_MILLIS / 2),
      createLoggingEvent("test.logger", Level.INFO, "1", baseTime - 10 * 1000),
      createLoggingEvent("test.logger", Level.INFO, "1", baseTime - 600),
      createLoggingEvent("test.logger", Level.INFO, "5", baseTime - 20 * 1000),
      createLoggingEvent("test.logger", Level.INFO, "5", baseTime - 20 * 1000 + EVENT_DELAY_MILLIS / 2),
      createLoggingEvent("test.logger", Level.INFO, "6", baseTime - 600),
      createLoggingEvent("test.logger", Level.INFO, "6", baseTime - 10 * 1000),
      createLoggingEvent("test.logger", Level.INFO, "7", baseTime - 2 * 1000 + EVENT_DELAY_MILLIS),
      createLoggingEvent("test.logger", Level.INFO, "8", baseTime - 7 * 1000 + EVENT_DELAY_MILLIS),
      createLoggingEvent("test.logger", Level.INFO, "4", baseTime - 100 + EVENT_DELAY_MILLIS));
    publishLog(topic, outOfOrderEvents);
    waitForAllLogsPublished(topic, outOfOrderEvents.size());

    KafkaOffsetResolver offsetResolver = new KafkaOffsetResolver(KAFKA_TESTER.getBrokerService(), config);
    // Use every event's timestamp as target time and assert that found offset with target timestamp
    // matches the expected offset
    for (ILoggingEvent event : outOfOrderEvents) {
      assertOffsetResolverResult(offsetResolver, outOfOrderEvents, event.getTimeStamp(), baseTime);
    }

    // Try to find the offset with an event time that has timestamp earlier than all event timestamps in Kafka
    assertOffsetResolverResult(offsetResolver, outOfOrderEvents,
                               baseTime - 10 * EVENT_DELAY_MILLIS, baseTime);

    // Try to find the offset with an event time that has timestamp larger than all event timestamps in Kafka
    assertOffsetResolverResult(offsetResolver, outOfOrderEvents,
                               baseTime + 10 * EVENT_DELAY_MILLIS, baseTime);

    // Use a random number between (timestamp - EVENT_DELAY_MILLIS, timestamp + EVENT_DELAY_MILLIS) as target time
    // and assert that found offset with target timestamp matches the expected offset
    for (int i = 0; i < 10; i++) {
      for (ILoggingEvent event : outOfOrderEvents) {
        assertOffsetResolverResult(offsetResolver, outOfOrderEvents,
                                   event.getTimeStamp() + RANDOM.nextInt() % EVENT_DELAY_MILLIS, baseTime);
      }
    }
  }

  @Test
  public void testInOrderEvents() throws InterruptedException, IOException {
    String topic = "testInOrderEvents";
    KafkaPipelineConfig config = new KafkaPipelineConfig(topic, Collections.singleton(0), 1024L, EVENT_DELAY_MILLIS,
                                                         1048576, 200L);
    KAFKA_TESTER.createTopic(topic, 1);

    // Publish log messages to Kafka and wait for all messages to be published
    long baseTime = System.currentTimeMillis() - EVENT_DELAY_MILLIS;
    List<ILoggingEvent> inOrderEvents = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      inOrderEvents.add(createLoggingEvent("test.logger", Level.INFO, Integer.toString(i), baseTime + i));
    }
    publishLog(topic, inOrderEvents);
    waitForAllLogsPublished(topic, inOrderEvents.size());

    KafkaOffsetResolver offsetResolver = new KafkaOffsetResolver(KAFKA_TESTER.getBrokerService(), config);
    // Use every event's timestamp as target time and assert that found offset is the next offset of the current offset
    for (int i = 0; i < inOrderEvents.size(); i++) {
      long targetTime = inOrderEvents.get(i).getTimeStamp();
      long offset = offsetResolver.getStartOffset(new Checkpoint(Long.MAX_VALUE, targetTime, 0), 0);
      Assert.assertEquals("Failed to find the expected event with the target time: " + targetTime,
                          i + 1, offset);
    }
  }

  private void waitForAllLogsPublished(String topic, int logsNum) throws InterruptedException {
    final CountDownLatch latch = new CountDownLatch(logsNum);
    final CountDownLatch stopLatch = new CountDownLatch(1);
    Cancellable cancel = KAFKA_TESTER.getKafkaClient().getConsumer().prepare().add(topic, 0, 0).consume(
      new KafkaConsumer.MessageCallback() {
        @Override
        public long onReceived(Iterator<FetchedMessage> messages) {
          long nextOffset = -1L;
          while (messages.hasNext()) {
            FetchedMessage message = messages.next();
            nextOffset = message.getNextOffset();
            Assert.assertTrue(latch.getCount() > 0);
            latch.countDown();
          }
          return nextOffset;
        }

        @Override
        public void finished() {
          stopLatch.countDown();
        }
      });

    Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
    cancel.cancel();
    Assert.assertTrue(stopLatch.await(1, TimeUnit.SECONDS));
  }

  private void assertOffsetResolverResult(KafkaOffsetResolver offsetResolver, List<ILoggingEvent> events,
                                          long targetTime, long baseTime) throws IOException {
    long expectedOffset = findExpectedOffsetByTime(events, targetTime);
    long offset = offsetResolver.getStartOffset(new Checkpoint(Long.MAX_VALUE, targetTime, 0), 0);
    Assert.assertEquals(String.format("Failed to find the expected event with the target time %d when basetime is %d ",
                                      targetTime, baseTime), expectedOffset, offset);
  }

  /**
   * Finds the smallest next offset with corresponding timestamp equal to targetTime,
   * or the largest next offset with corresponding timestamp smaller than (targetTime - EVENT_DELAY_MILLIS) if
   * no event has timestamp equal to targetTime, or the smallest offset 0 if no event has timestamp smaller
   * than (targetTime - EVENT_DELAY_MILLIS)
   */
  private long findExpectedOffsetByTime(List<ILoggingEvent> events, long targetTime) {
    long offset = 0;
    // Use smallest offset 0 as default value in case no event has timestamp smaller
    long latestOffsetBeforeMinTime = 0;
    long minTime = targetTime - EVENT_DELAY_MILLIS;
    for (ILoggingEvent event : events) {
      long time = event.getTimeStamp();
      if (time == targetTime) {
        // Increment the offset to get the next offset
        return offset + 1;
      }
      if (time < minTime) {
        // Increment the offset to get the next offset
        latestOffsetBeforeMinTime = offset + 1;
      }
      offset++;
    }
    // No event contains timestamp equal to targetTime, return latestOffsetBeforeMinTime
    return latestOffsetBeforeMinTime;
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
      preparer.add(ByteBuffer.wrap(serializer.toBytes(event)), context.getLogPartition());
    }
    preparer.send();
  }
}
