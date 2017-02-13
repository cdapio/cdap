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
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.kafka.KafkaTester;
import co.cask.cdap.logging.appender.kafka.LoggingEventSerializer;
import co.cask.cdap.logging.context.GenericLoggingContext;
import co.cask.cdap.logging.meta.Checkpoint;
import co.cask.cdap.metrics.collect.LocalMetricsCollectionService;
import co.cask.cdap.metrics.guice.MetricsStoreModule;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Unit-test for {@link KafkaOffsetResolver}.
 */
public class KafkaOffsetResolverTest {
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
                      new MetricsStoreModule(),
                      new AbstractModule() {
                        @Override
                        protected void configure() {
                          bind(MetricsCollectionService.class).to(LocalMetricsCollectionService.class);
                        }
                      }),
                    1);

  @Test
  public void testFindOffset() throws Exception {
    String topic = "testOffsetResolver";
    long replicationDelayMillis = 60000L;
    long eventOutOfOrderMillis = 120000L;
    KafkaPipelineConfig config = new KafkaPipelineConfig(topic, Collections.singleton(0), 1024L, 100L,
                                                         replicationDelayMillis, eventOutOfOrderMillis,
                                                         1048576, 200L);
    KAFKA_TESTER.createTopic(topic, 1);

    // Publish some log messages to Kafka
    long baseTime = System.currentTimeMillis() - replicationDelayMillis;
    List<ILoggingEvent> events = ImmutableList.of(
      createLoggingEvent("test.logger", Level.INFO, "0", baseTime - 20 * 1000 - eventOutOfOrderMillis),
      createLoggingEvent("test.logger", Level.INFO, "0", baseTime - 20 * 1000 - eventOutOfOrderMillis),
      createLoggingEvent("test.logger", Level.INFO, "1", baseTime - 7 * 1000 - eventOutOfOrderMillis),
      createLoggingEvent("test.logger", Level.INFO, "2", baseTime - 9 * 100),
      createLoggingEvent("test.logger", Level.INFO, "3", baseTime - 500),
      createLoggingEvent("test.logger", Level.INFO, "1", baseTime - 9 * 1000),
      createLoggingEvent("test.logger", Level.INFO, "1", baseTime - 9 * 1000 + eventOutOfOrderMillis / 2),
      createLoggingEvent("test.logger", Level.INFO, "1", baseTime - 9 * 1000),
      createLoggingEvent("test.logger", Level.INFO, "1", baseTime - 9 * 1000 - eventOutOfOrderMillis / 2),
      createLoggingEvent("test.logger", Level.INFO, "1", baseTime - 10 * 1000),
      createLoggingEvent("test.logger", Level.INFO, "1", baseTime - 600),
      createLoggingEvent("test.logger", Level.INFO, "5", baseTime - 20 * 1000),
      createLoggingEvent("test.logger", Level.INFO, "5", baseTime - 20 * 1000 + eventOutOfOrderMillis / 2),
      createLoggingEvent("test.logger", Level.INFO, "6", baseTime - 600),
      createLoggingEvent("test.logger", Level.INFO, "6", baseTime - 10 * 1000),
      createLoggingEvent("test.logger", Level.INFO, "7", baseTime - 16 * 1000 + eventOutOfOrderMillis),
      createLoggingEvent("test.logger", Level.INFO, "8", baseTime - 7 * 1000 + eventOutOfOrderMillis),
      createLoggingEvent("test.logger", Level.INFO, "4", baseTime - 100 + eventOutOfOrderMillis));
    publishLog(topic, events);
    KafkaOffsetResolver offsetResolver = new KafkaOffsetResolver(KAFKA_TESTER.getBrokerService(), config);

    final CountDownLatch latch = new CountDownLatch(events.size());
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

    // Use every event's timestamp as target time and assert that found offset with target timestamp
    // matches the expected offset
    for (int i = 0; i < events.size(); i++) {
      long targetTime = events.get(i).getTimeStamp();
      long offset = offsetResolver.getMatchingOffset(new Checkpoint(Long.MAX_VALUE, targetTime, 0), 0);
      // Increment the offset returned by findSmallestOffsetByTime to get the next offset
      long expectedOffset = findSmallestOffsetByTime(events, targetTime) + 1;
      Assert.assertEquals(expectedOffset, offset);
    }
    // Failed to find matching offset for timestamp returns -1
    Assert.assertEquals(-1, offsetResolver.getMatchingOffset(new Checkpoint(Long.MAX_VALUE, Long.MAX_VALUE, 0), 0));
  }

  /**
   * Finds the smallest offset with corresponding timestamp equal to targetTime.
   */
  private long findSmallestOffsetByTime(List<ILoggingEvent> events, long targetTime) {
    long offset = 0;
    for (ILoggingEvent event : events) {
      if (event.getTimeStamp() == targetTime) {
        return offset;
      }
      offset++;
    }
    // should never reach here
    return -1;
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
      preparer.add(ByteBuffer.wrap(serializer.toBytes(event, context)), context.getLogPartition());
    }
    preparer.send();
  }
}
