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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.logging.appender.AbstractLogPublisher;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogMessage;
import co.cask.cdap.logging.serialize.LoggingEventSerializer;
import com.google.inject.Inject;
import kafka.producer.KeyedMessage;

import java.util.List;

/**
 * Log appender that publishes log messages to Kafka.
 */
public final class KafkaLogAppender extends LogAppender {

  private static final int QUEUE_SIZE = 512;

  private static final String APPENDER_NAME = "KafkaLogAppender";

  private final KafkaLogPublisher kafkaLogPublisher;

  @Inject
  KafkaLogAppender(CConfiguration cConf) {
    setName(APPENDER_NAME);
    this.kafkaLogPublisher = new KafkaLogPublisher(cConf);
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
      kafkaLogPublisher.addMessage(logMessage);
    } catch (InterruptedException e) {
      addInfo("Interrupted when adding log message to queue: " + logMessage.getFormattedMessage());
    }
  }

  /**
   * Publisher service to publish logs to Kafka asynchronously.
   */
  private final class KafkaLogPublisher extends AbstractLogPublisher<KeyedMessage<String, byte[]>> {

    private final CConfiguration cConf;
    private final String topic;
    private final LoggingEventSerializer loggingEventSerializer;
    private final LogPartitionType logPartitionType;
    private SimpleKafkaProducer producer;

    private KafkaLogPublisher(CConfiguration cConf) {
      super(QUEUE_SIZE, RetryStrategies.fromConfiguration(cConf, "system.log.process."));
      this.cConf = cConf;
      this.topic = cConf.get(Constants.Logging.KAFKA_TOPIC);
      this.loggingEventSerializer = new LoggingEventSerializer();
      this.logPartitionType =
              LogPartitionType.valueOf(cConf.get(Constants.Logging.LOG_PUBLISH_PARTITION_KEY).toUpperCase());
    }

    @Override
    protected void doStartUp() throws Exception {
      producer = new SimpleKafkaProducer(cConf);
      super.doStartUp();
    }

    @Override
    protected void doShutdown() throws Exception {
      super.doShutdown();
      producer.stop();
    }

    /**
     * Creates a {@link KeyedMessage} for the given {@link LogMessage}.
     */
    @Override
    protected KeyedMessage<String, byte[]> createMessage(LogMessage logMessage) {
      String partitionKey = logPartitionType.getPartitionKey(logMessage.getLoggingContext());
      return new KeyedMessage<>(topic, partitionKey, loggingEventSerializer.toBytes(logMessage));
    }

    @Override
    protected void publish(List<KeyedMessage<String, byte[]>> logMessages) {
      producer.publish(logMessages);
    }

    @Override
    protected void logError(String errorMessage, Exception exception) {
      // Log using the status manager
      addError(errorMessage, exception);
    }
  }
}
