/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.logging.appender.kafka;

import ch.qos.logback.core.spi.ContextAware;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.ApplicationLoggingContext;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.NamespaceLoggingContext;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogMessage;
import co.cask.cdap.logging.serialize.LoggingEventSerializer;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Inject;
import kafka.producer.KeyedMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Log appender that publishes log messages to Kafka.
 */
public final class KafkaLogAppender extends LogAppender {

  private static final int QUEUE_SIZE = 512;

  private static final String APPENDER_NAME = "KafkaLogAppender";

  private final BlockingQueue<LogMessage> messageQueue;
  private final KafkaLogPublisher kafkaLogPublisher;

  @Inject
  KafkaLogAppender(CConfiguration cConf) {
    setName(APPENDER_NAME);
    this.messageQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
    this.kafkaLogPublisher = new KafkaLogPublisher(cConf, messageQueue, this);
  }

  @Override
  public void start() {
    kafkaLogPublisher.startAndWait();
    addInfo("Successfully initialized KafkaLogAppender.");
    super.start();
  }

  @Override
  public void stop() {
    kafkaLogPublisher.stopAndWait();
    super.stop();
  }

  @Override
  protected void appendEvent(LogMessage logMessage) {
    logMessage.prepareForDeferredProcessing();
    logMessage.getCallerData();

    try {
      messageQueue.put(logMessage);
    } catch (InterruptedException e) {
      addInfo("Interrupted when adding log message to queue: " + logMessage.getFormattedMessage());
    }
  }

  /**
   * Publisher service to publish logs to Kafka asynchronously.
   */
  private static final class KafkaLogPublisher extends AbstractExecutionThreadService {

    private final CConfiguration cConf;
    private final BlockingQueue<LogMessage> messageQueue;
    private final String topic;
    private final LoggingEventSerializer loggingEventSerializer;
    private final ContextAware contextAware;
    private final RetryStrategy retryStrategy;
    private SimpleKafkaProducer producer;
    private volatile Thread blockingThread;

    private KafkaLogPublisher(CConfiguration cConf, BlockingQueue<LogMessage> messageQueue, ContextAware contextAware) {
      this.cConf = cConf;
      this.messageQueue = messageQueue;
      this.topic = cConf.get(Constants.Logging.KAFKA_TOPIC);
      this.loggingEventSerializer = new LoggingEventSerializer();
      this.contextAware = contextAware;
      this.retryStrategy = RetryStrategies.fromConfiguration(cConf, "system.log.process.");
    }

    @Override
    protected void startUp() throws Exception {
      producer = new SimpleKafkaProducer(cConf);
    }

    @Override
    protected void shutDown() throws Exception {
      producer.stop();
    }

    @Override
    protected void run() {
      List<KeyedMessage<String, byte[]>> buffer = new ArrayList<>(QUEUE_SIZE);

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

            // Log using the status manager
            contextAware.addError("Failed to publish log message to Kafka on topic " + topic, e);
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
          contextAware.addError("Failed to publish log message to Kafka on topic " + topic, e);
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
    }

    @Override
    protected Executor executor() {
      return new Executor() {
        @Override
        public void execute(Runnable command) {
          Thread thread = new Thread(command, "kafka-log-publisher");
          thread.setDaemon(true);
          thread.start();
        }
      };
    }

    /**
     * Publishes messages from the message queue to Kafka.
     *
     * @param buffer a buffer for storing {@link KeyedMessage} for publishing to Kafka
     * @throws InterruptedException if the thread is interrupted
     */
    private void publishMessages(List<KeyedMessage<String, byte[]>> buffer,
                                 boolean blockForMessage) throws InterruptedException {
      int maxBufferSize = QUEUE_SIZE;

      if (blockForMessage) {
        blockingThread = Thread.currentThread();
        try {
          if (isRunning()) {
            buffer.add(createKeyedMessage(messageQueue.take()));
            maxBufferSize--;
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
        buffer.add(createKeyedMessage(message));
      }

      // Publish all messages
      producer.publish(buffer);
    }

    /**
     * Creates a {@link KeyedMessage} for the given {@link LogMessage}.
     */
    private KeyedMessage<String, byte[]> createKeyedMessage(LogMessage logMessage) {
      String partitionKey = getPartitionKey(logMessage.getLoggingContext());
      return new KeyedMessage<>(topic, partitionKey, loggingEventSerializer.toBytes(logMessage));
    }

    /**
     * Computes the Kafka partition key based on the given {@link LoggingContext}.
     */
    private String getPartitionKey(LoggingContext loggingContext) {
      String namespaceId = loggingContext.getSystemTagsMap().get(NamespaceLoggingContext.TAG_NAMESPACE_ID).getValue();

      if (NamespaceId.SYSTEM.getNamespace().equals(namespaceId)) {
        return loggingContext.getLogPartition();
      }

      switch (LogPartitionType.valueOf(cConf.get(Constants.Logging.LOG_PUBLISH_PARTITION_KEY).toUpperCase())) {
        case PROGRAM:
          return loggingContext.getLogPartition();
        case APPLICATION:
          return namespaceId + ":" +
            loggingContext.getSystemTagsMap().get(ApplicationLoggingContext.TAG_APPLICATION_ID).getValue();
        default:
          // this should never happen
          throw new IllegalArgumentException(
            String.format("Invalid log partition type %s. Allowed partition types are program/application",
                          cConf.get(Constants.Logging.LOG_PUBLISH_PARTITION_KEY)));
      }
    }
  }
}
