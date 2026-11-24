/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.logging.appender;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.LoggingEvent;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.logging.context.WorkerLoggingContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link AbstractLogPublisher}.
 */
public class AbstractLogPublisherTest {

  @Test
  public void testStartPublishAndShutdownLifecycle() throws Exception {
    final List<String> published = Collections.synchronizedList(new ArrayList<String>());
    final AtomicReference<String> publishThreadName = new AtomicReference<>();
    final CountDownLatch firstPublishLatch = new CountDownLatch(1);

    TestLogPublisher publisher = new TestLogPublisher(16, RetryStrategies.noRetry()) {
      @Override
      protected void publish(List<String> logMessages) throws Exception {
        publishThreadName.compareAndSet(null, Thread.currentThread().getName());
        if (!logMessages.isEmpty()) {
          published.addAll(logMessages);
          firstPublishLatch.countDown();
          TimeUnit.MILLISECONDS.sleep(50);
        }
      }
    };

    publisher.startAsync().awaitRunning();
    Assert.assertTrue(publisher.isRunning());
    publisher.addMessage(createLogMessage("first-msg"));
    Assert.assertTrue(firstPublishLatch.await(5, TimeUnit.SECONDS));
    publisher.stopAsync().awaitTerminated();

    Assert.assertEquals(Collections.singletonList("first-msg"), published);
    Assert.assertNotNull(publishThreadName.get());
    Assert.assertTrue(publishThreadName.get().contains("TestLogPublisher"));
  }

  @Test
  public void testRetryFailureAndExhaustedLogsServiceName() throws Exception {
    final List<String> recordedErrors = Collections.synchronizedList(new ArrayList<String>());
    final CountDownLatch exhaustedLatch = new CountDownLatch(1);
    final IOException failure = new IOException("simulated publish outage");

    TestLogPublisher publisher = new TestLogPublisher(
        16, RetryStrategies.limit(1, RetryStrategies.fixDelay(1, TimeUnit.MILLISECONDS))) {
      @Override
      protected void publish(List<String> logMessages) throws Exception {
        if (!logMessages.isEmpty()) {
          throw failure;
        }
      }

      @Override
      protected void logError(String errorMessage, Exception exception) {
        recordedErrors.add(errorMessage);
        Assert.assertSame(failure, exception);
        exhaustedLatch.countDown();
        try {
          TimeUnit.MILLISECONDS.sleep(50);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    };

    publisher.startAsync().awaitRunning();
    publisher.addMessage(createLogMessage("failing-msg"));
    Assert.assertTrue(exhaustedLatch.await(5, TimeUnit.SECONDS));
    publisher.stopAsync().awaitTerminated();

    Assert.assertFalse(recordedErrors.isEmpty());
    Assert.assertEquals("Failed to publish log message by TestLogPublisher", recordedErrors.get(0));
  }

  @Test
  public void testShutdownFailureLogsServiceNameAndClearsQueue() throws Exception {
    final CountDownLatch blockInRunLatch = new CountDownLatch(1);
    final CountDownLatch unblockRunLatch = new CountDownLatch(1);
    final AtomicBoolean failOnShutdown = new AtomicBoolean(false);
    final List<String> recordedErrors = Collections.synchronizedList(new ArrayList<String>());
    final List<String> failedBatch = Collections.synchronizedList(new ArrayList<String>());

    TestLogPublisher publisher = new TestLogPublisher(16, RetryStrategies.noRetry()) {
      @Override
      protected void publish(List<String> logMessages) throws Exception {
        if (failOnShutdown.get()) {
          failedBatch.addAll(logMessages);
          throw new IOException("shutdown flush failed");
        }
        if (!logMessages.isEmpty()) {
          blockInRunLatch.countDown();
          unblockRunLatch.await(5, TimeUnit.SECONDS);
        }
      }

      @Override
      protected void logError(String errorMessage, Exception exception) {
        recordedErrors.add(errorMessage);
      }
    };

    publisher.startAsync().awaitRunning();
    publisher.addMessage(createLogMessage("in-flight"));
    Assert.assertTrue(blockInRunLatch.await(5, TimeUnit.SECONDS));

    publisher.addMessage(createLogMessage("queued-for-shutdown"));
    failOnShutdown.set(true);
    // Request the stop while the run thread is still blocked inside publish(). This cancels any
    // further scheduled iteration, so the queued message can only be published by doShutdown().
    // Stopping after unblocking races with the run loop, which retries the failing publish in a
    // tight loop and records one error per attempt.
    publisher.stopAsync();
    unblockRunLatch.countDown();
    publisher.awaitTerminated(10, TimeUnit.SECONDS);

    Assert.assertEquals(Collections.singletonList("queued-for-shutdown"), failedBatch);
    Assert.assertEquals(
        Collections.singletonList("Failed to publish log message by TestLogPublisher"),
        recordedErrors);
  }

  private static LogMessage createLogMessage(String text) {
    LoggingEvent event = new LoggingEvent();
    event.setLevel(Level.INFO);
    event.setLoggerName("test.logger");
    event.setMessage(text);
    event.setTimeStamp(1700000000000L);
    return new LogMessage(
        event, new WorkerLoggingContext("default", "app1", "worker1", "run1", "0"));
  }

  private abstract static class TestLogPublisher extends AbstractLogPublisher<String> {

    TestLogPublisher(int queueSize, RetryStrategy retryStrategy) {
      super(queueSize, retryStrategy);
    }

    @Override
    protected String getServiceName() {
      return "TestLogPublisher";
    }

    @Override
    protected String createMessage(LogMessage logMessage) {
      return logMessage.getFormattedMessage();
    }
  }
}
