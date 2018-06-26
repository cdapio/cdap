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

package co.cask.cdap.logging.appender.tms;

import ch.qos.logback.core.spi.ContextAware;
import co.cask.cdap.api.messaging.MessagePublisher;
import co.cask.cdap.api.messaging.MessagingContext;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.ApplicationLoggingContext;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.NamespaceLoggingContext;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogMessage;
import co.cask.cdap.logging.appender.kafka.LogPartitionType;
import co.cask.cdap.logging.appender.kafka.StringPartitioner;
import co.cask.cdap.logging.serialize.LoggingEventSerializer;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.context.MultiThreadMessagingContext;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Log appender that publishes log messages to TMS.
 */
public final class TMSLogAppender extends LogAppender {

  private static final int QUEUE_SIZE = 512;

  private static final String APPENDER_NAME = "TMSLogAppender";

  private final BlockingQueue<LogMessage> messageQueue;
  private final TMSLogPublisher tmsLogPublisher;

  @Inject
  TMSLogAppender(CConfiguration cConf, MessagingService messagingService) {
    setName(APPENDER_NAME);
    this.messageQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
    this.tmsLogPublisher = new TMSLogPublisher(cConf, messageQueue, this, messagingService);
  }

  @Override
  public void start() {
    tmsLogPublisher.startAndWait();
    addInfo("Successfully initialized TMSLogAppender.");
    super.start();
  }

  @Override
  public void stop() {
    tmsLogPublisher.stopAndWait();
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
   * Publisher service to publish logs to TMS asynchronously.
   */
  private static final class TMSLogPublisher extends AbstractExecutionThreadService {

    private final CConfiguration cConf;
    private final BlockingQueue<LogMessage> messageQueue;
    private final String topicPrefix;
    private final int numPartitions;
    private final LoggingEventSerializer loggingEventSerializer;
    private final ContextAware contextAware;
    private final MessagingContext messagingContext;
    private final RetryStrategy retryStrategy;
//    private final StringPartitioner stringPartitioner;
    private volatile Thread blockingThread;

    private TMSLogPublisher(CConfiguration cConf, BlockingQueue<LogMessage> messageQueue, ContextAware contextAware,
                            MessagingService messagingService) {
      this.cConf = cConf;
      this.messageQueue = messageQueue;
      // TODO: rename variable
      this.topicPrefix = cConf.get(Constants.Logging.TMS_TOPIC_PREFIX);
      this.numPartitions = cConf.getInt(Constants.Logging.NUM_PARTITIONS);
      this.loggingEventSerializer = new LoggingEventSerializer();
      this.contextAware = contextAware;
      this.retryStrategy = RetryStrategies.fromConfiguration(cConf, "system.log.process.");
      this.messagingContext = new MultiThreadMessagingContext(messagingService);
//      this.stringPartitioner = new StringPartitioner(cConf);
    }

    @Override
    protected void run() {
      Map<Integer, List<byte[]>> buffer = new HashMap<>(QUEUE_SIZE);

      int failures = 0;
      long failureStartTime = System.currentTimeMillis();
      while (isRunning()) {
        try {
          // Only block for messages if it is not a failure retry
          publishMessages(buffer, failures == 0);
          // Any exception from the publishMessages call meaning messages are not yet published to TMS,
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
            contextAware.addError("Failed to publish log message to TMS on topicPrefix " + topicPrefix, e);
            e.printStackTrace();
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
          contextAware.addError("Failed to publish log message to TMS on topicPrefix " + topicPrefix, e);
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
          Thread thread = new Thread(command, "tms-log-publisher");
          thread.setDaemon(true);
          thread.start();
        }
      };
    }

    /**
     * Publishes messages from the message queue to TMS.
     *
     * @param buffer a buffer for storing {@code byte[]} for publishing to TMS
     * @throws InterruptedException if the thread is interrupted
     */
    private void publishMessages(Map<Integer, List<byte[]>> buffer, boolean blockForMessage)
            throws IOException, InterruptedException, TopicNotFoundException {
      int bufferSize = 0;
      for (Map.Entry<Integer, List<byte[]>> partitionBuffer : buffer.entrySet()) {
        bufferSize += partitionBuffer.getValue().size();
      }

      if (blockForMessage) {
        blockingThread = Thread.currentThread();
        try {
          if (isRunning()) {
            serializeMessageToBuffer(messageQueue.take(), buffer);
            bufferSize++;
          }
        } catch (InterruptedException e) {
          // just ignore and keep going. This happen when this publisher is getting shutdown, but we still want
          // to publish all pending messages.
        } finally {
          blockingThread = null;
        }
      }

      while (bufferSize < QUEUE_SIZE) {
        // Poll for more messages
        LogMessage message = messageQueue.poll();
        if (message == null) {
          break;
        }
        serializeMessageToBuffer(message, buffer);
        bufferSize++;
      }

      MessagePublisher directMessagePublisher = messagingContext.getDirectMessagePublisher();
      // Publish all messages
      for (Map.Entry<Integer, List<byte[]>> partitionBuffer : buffer.entrySet()) {
        Integer partition = Preconditions.checkNotNull(partitionBuffer.getKey());
        List<byte[]> payload = partitionBuffer.getValue();
        directMessagePublisher.publish(NamespaceId.SYSTEM.getNamespace(),
                                       topicPrefix + partition, payload.iterator());
      }
    }

    private void serializeMessageToBuffer(LogMessage logMessage, Map<Integer, List<byte[]>> buffer) {
      String partitionKey = getPartitionKey(logMessage.getLoggingContext());
//      int partition = stringPartitioner.partition(partitionKey, numPartitions);
      int partition = partition(partitionKey, numPartitions);

      if (!buffer.containsKey(partition)) {
        buffer.put(partition, new ArrayList<>());
      }
      buffer.get(partition).add(loggingEventSerializer.toBytes(logMessage));
    }

    // Based off of StringPartitioner, but that class can not be used in Standalone, as kafka dependencies do not exist
    // in Standalone
    private int partition(Object key, int numPartitions) {
      return Math.abs(Hashing.md5().hashString(key.toString()).asInt()) % numPartitions;
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
