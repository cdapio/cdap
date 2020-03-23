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

package io.cdap.cdap.logging.appender.tms;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import io.cdap.cdap.api.messaging.MessagePublisher;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.logging.appender.AbstractLogPublisher;
import io.cdap.cdap.logging.appender.LogAppender;
import io.cdap.cdap.logging.appender.LogMessage;
import io.cdap.cdap.logging.appender.kafka.LogPartitionType;
import io.cdap.cdap.logging.serialize.LoggingEventSerializer;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.proto.id.NamespaceId;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Log appender that publishes log messages to TMS.
 */
public final class TMSLogAppender extends LogAppender {

  private static final String APPENDER_NAME = "TMSLogAppender";

  private final TMSLogPublisher tmsLogPublisher;

  @Inject
  TMSLogAppender(CConfiguration cConf, MessagingService messagingService) {
    setName(APPENDER_NAME);
    int queueSize = cConf.getInt(Constants.Logging.APPENDER_QUEUE_SIZE);
    this.tmsLogPublisher = new TMSLogPublisher(cConf, messagingService, queueSize);
  }

  @Override
  public void start() {
    tmsLogPublisher.startAndWait();
    addInfo("Successfully started " + APPENDER_NAME);
    super.start();
  }

  @Override
  public void stop() {
    tmsLogPublisher.stopAndWait();
    addInfo("Successfully stopped " + APPENDER_NAME);
    super.stop();
  }

  @Override
  protected void appendEvent(LogMessage logMessage) {
    logMessage.prepareForDeferredProcessing();
    logMessage.getCallerData();

    try {
      tmsLogPublisher.addMessage(logMessage);
    } catch (InterruptedException e) {
      addInfo("Interrupted when adding log message to queue: " + logMessage.getFormattedMessage());
    }
  }

  // Based off of StringPartitioner, but that class can not be used in Standalone, as kafka dependencies do not exist
  // in Standalone
  @VisibleForTesting
  static int partition(Object key, int numPartitions) {
    return Math.abs(Hashing.md5().hashString(key.toString()).asInt()) % numPartitions;
  }

  /**
   * Publisher service to publish logs to TMS asynchronously.
   */
  private final class TMSLogPublisher extends AbstractLogPublisher<Map.Entry<Integer, byte[]>> {

    private final String topicPrefix;
    private final int numPartitions;
    private final LoggingEventSerializer loggingEventSerializer;
    private final MessagingContext messagingContext;
    private final LogPartitionType logPartitionType;

    private TMSLogPublisher(CConfiguration cConf, MessagingService messagingService, int queueSize) {
      super(queueSize, RetryStrategies.fromConfiguration(cConf, "system.log.process."));
      this.topicPrefix = cConf.get(Constants.Logging.TMS_TOPIC_PREFIX);
      this.numPartitions = cConf.getInt(Constants.Logging.NUM_PARTITIONS);
      this.loggingEventSerializer = new LoggingEventSerializer();
      this.logPartitionType =
              LogPartitionType.valueOf(cConf.get(Constants.Logging.LOG_PUBLISH_PARTITION_KEY).toUpperCase());
      this.messagingContext = new MultiThreadMessagingContext(messagingService);
    }

    @Override
    protected Map.Entry<Integer, byte[]> createMessage(LogMessage logMessage) {
      String partitionKey = logPartitionType.getPartitionKey(logMessage.getLoggingContext());
      int partition = partition(partitionKey, numPartitions);
      return new AbstractMap.SimpleEntry<>(partition, loggingEventSerializer.toBytes(logMessage));
    }

    @Override
    protected void publish(List<Map.Entry<Integer, byte[]>> logMessages) throws TopicNotFoundException, IOException {
      MessagePublisher directMessagePublisher = messagingContext.getDirectMessagePublisher();

      // Group the log messages by partition and then publish all messages to their respective partitions
      Map<Integer, List<byte[]>> partitionedMessages = new HashMap<>();
      for (Map.Entry<Integer, byte[]> logMessage : logMessages) {
        List<byte[]> messages = partitionedMessages.computeIfAbsent(logMessage.getKey(), k -> new ArrayList<>());
        messages.add(logMessage.getValue());
      }

      for (Map.Entry<Integer, List<byte[]>> partition : partitionedMessages.entrySet()) {
        directMessagePublisher.publish(NamespaceId.SYSTEM.getNamespace(),
                                       topicPrefix + partition.getKey(), partition.getValue().iterator());
      }
    }

    @Override
    protected void logError(String errorMessage, Exception exception) {
      // Log using the status manager
      addError(errorMessage, exception);
    }
  }
}
