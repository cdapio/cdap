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
import co.cask.cdap.common.logging.ApplicationLoggingContext;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.NamespaceLoggingContext;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogMessage;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Log appender that publishes log messages to Kafka.
 */
public final class KafkaLogAppender extends LogAppender {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaLogAppender.class);

  private static final String APPENDER_NAME = "KafkaLogAppender";

  private final SimpleKafkaProducer producer;
  private final LoggingEventSerializer loggingEventSerializer;

  private final CConfiguration cConf;

  private final AtomicBoolean stopped = new AtomicBoolean(false);

  @Inject
  KafkaLogAppender(CConfiguration cConf) {
    setName(APPENDER_NAME);
    addInfo("Initializing KafkaLogAppender...");

    this.cConf = cConf;
    this.producer = new SimpleKafkaProducer(cConf);
    this.loggingEventSerializer = new LoggingEventSerializer();
    addInfo("Successfully initialized KafkaLogAppender.");
  }

  @Override
  protected void appendEvent(LogMessage logMessage) {
    try {
      String partitionKey = getPartitionKey(logMessage.getLoggingContext());
      // publish only if its a valid type of partition key
      if (!Strings.isNullOrEmpty(partitionKey)) {
        byte [] bytes = loggingEventSerializer.toBytes(logMessage);
        producer.publish(partitionKey, bytes);
      }
    } catch (Throwable t) {
      LOG.error("Got exception while serializing log event {}.", logMessage, t);
    }
  }

  private String getPartitionKey(LoggingContext loggingContext) {
    String namespaceId = loggingContext.getSystemTagsMap().get(NamespaceLoggingContext.TAG_NAMESPACE_ID).getValue();

    if (NamespaceId.SYSTEM.getNamespace().equals(namespaceId)) {
      return loggingContext.getLogPartition();
    }

    switch (LogPartitionType.valueOf(cConf.get(Constants.Logging.LOG_PUBLISH_PARTITION_KEY).toUpperCase())) {
      case PROGRAM:
        return loggingContext.getLogPartition();
      case APPLICATION:
        return namespaceId + ":" + loggingContext.getSystemTagsMap().get(ApplicationLoggingContext
                                                                           .TAG_APPLICATION_ID).getValue();
      default:
        // this should never happen
        throw new IllegalArgumentException(String.format("Invalid log partition type %s. Allowed partition types " +
                                                           "are program/application",
                                                         cConf.get(Constants.Logging.LOG_PUBLISH_PARTITION_KEY)));
    }
  }

  @Override
  public void stop() {
    if (!stopped.compareAndSet(false, true)) {
      return;
    }

    super.stop();
    producer.stop();
  }
}
