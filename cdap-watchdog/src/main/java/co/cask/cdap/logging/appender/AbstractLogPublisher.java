/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.logging.appender;

import co.cask.cdap.common.service.RetryStrategy;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import kafka.producer.KeyedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A class that continuously serializes logs from a queue and publishes them.
 *
 * @param <MESSAGE> the type of message used in the in-memory buffer, before publishing
 */
public abstract class AbstractLogPublisher<MESSAGE> extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractLogPublisher.class);

  private final RetryStrategy retryStrategy;
  private final int queueSize;
  private final BlockingQueue<LogMessage> messageQueue;
  private final List<MESSAGE> buffer;
  private volatile Thread blockingThread;

  public AbstractLogPublisher(int queueSize, RetryStrategy retryStrategy) {
    this.retryStrategy = retryStrategy;
    this.queueSize = queueSize;
    this.messageQueue = new ArrayBlockingQueue<>(queueSize);
    this.buffer = new ArrayList<>(queueSize);
  }

  /**
   * Creates a {@link MESSAGE}, which the type is dependent on the implementation. This is buffered in-memory before
   * being published.
   *
   * @param logMessage the log message to prepare for publishing
   * @return a {@link MESSAGE}, prepared for publishing.
   */
  protected abstract MESSAGE createMessage(LogMessage logMessage);

  /**
   * Responsible for publishing a list of log messages.
   *
   * @param logMessages the list of log messages to be published.
   */
  protected abstract void publish(List<MESSAGE> logMessages) throws Exception;

  /**
   * Adds a log message for publishing.
   *
   * @param logMessage the log message to add for publishing
   */
  public final void addMessage(LogMessage logMessage) throws InterruptedException {
    messageQueue.put(logMessage);
  }

  @Override
  protected void run() throws Exception {
    int failures = 0;
    long failureStartTime = System.currentTimeMillis();
    while (isRunning()) {
      try {
        // Only block for messages if it is not a failure retry
        publishMessages(buffer, failures == 0);
        // Any exception from the publishMessages call meaning messages are not yet published to Kafka,
        // hence not clearing the buffer
        buffer.clear();
        failures = 0;
      } catch (InterruptedException e) {
        break;
      } catch (Exception e) {
        if (failures == 0) {
          failureStartTime = System.currentTimeMillis();
        }

        long sleepMillis = retryStrategy.nextRetry(++failures, failureStartTime);
        if (sleepMillis < 0) {
          buffer.clear();
          failures = 0;

          logError("Failed to publish log message by " + getServiceName(), e);
        } else {
          blockingThread = Thread.currentThread();
          try {
            if (isRunning()) {
              TimeUnit.MILLISECONDS.sleep(sleepMillis);
            }
          } catch (InterruptedException ie) {
            break;
          } finally {
            blockingThread = null;
          }
        }
      }
    }

    // Publish all remaining messages.
    while (!messageQueue.isEmpty() || !buffer.isEmpty()) {
      try {
        publishMessages(buffer, false);
      } catch (Exception e) {
        logError("Failed to publish log message by " + getServiceName(), e);
      }
      // Ignore those that cannot be publish since we are already in shutdown sequence
      buffer.clear();
    }
  }

  @Override
  protected void triggerShutdown() {
    // Interrupt the run thread first
    // If the run loop is sleeping / blocking, it will wake and break the loop
    Thread runThread = this.blockingThread;
    if (runThread != null) {
      runThread.interrupt();
    }
    // Clear the interrupt flag.
    Thread.interrupted();
  }

  /**
   * Logs an error message and exception, depending on the capabilities of the subclass.
   *
   * @param errorMessage the error message to be logged
   * @param exception the exception that was encountered, resulting in the call to this method
   */
  protected void logError(String errorMessage, Exception exception) {
    LOG.error(errorMessage, exception);
  }

  /**
   * Publishes messages from the message queue to Kafka.
   *
   * @param buffer a buffer for storing {@link KeyedMessage} for publishing to Kafka
   * @throws InterruptedException if the thread is interrupted
   */
  private void publishMessages(List<MESSAGE> buffer,
                               boolean blockForMessage) throws Exception {
    int maxBufferSize = queueSize;

    if (blockForMessage) {
      blockingThread = Thread.currentThread();
      try {
        if (isRunning()) {
          LogMessage logMessage = messageQueue.poll(10, TimeUnit.SECONDS);
          if (logMessage != null) {
            buffer.add(createMessage(logMessage));
            maxBufferSize--;
          }
        }
      } catch (InterruptedException e) {
        // just ignore and keep going. This happen when this publisher is getting shutdown, but we still want
        // to publish all pending messages.
      } finally {
        blockingThread = null;
      }
    }

    while (buffer.size() < maxBufferSize) {
      // Poll for more messages
      LogMessage message = messageQueue.poll();
      if (message == null) {
        break;
      }
      buffer.add(createMessage(message));
    }

    // Publish all messages
    publish(buffer);
  }
}
