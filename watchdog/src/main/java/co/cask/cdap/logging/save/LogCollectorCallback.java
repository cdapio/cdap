/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.logging.save;

import ch.qos.logback.classic.spi.ILoggingEvent;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.logging.appender.kafka.LoggingEventSerializer;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.kafka.KafkaLogEvent;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import org.apache.avro.generic.GenericRecord;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * Kafka callback to fetch log messages and store them in time buckets per logging context.
 */
public class LogCollectorCallback implements KafkaConsumer.MessageCallback {
  private static final Logger LOG = LoggerFactory.getLogger(LogCollectorCallback.class);

  private final Table<Long, String, List<KafkaLogEvent>> messageTable;
  private final LoggingEventSerializer serializer;
  private final long eventBucketIntervalMs;

  public LogCollectorCallback(Table<Long, String, List<KafkaLogEvent>> messageTable, LoggingEventSerializer serializer,
                              long eventBucketIntervalMs) {
    this.messageTable = messageTable;
    this.serializer = serializer;
    this.eventBucketIntervalMs = eventBucketIntervalMs;
  }

  @Override
  public void onReceived(Iterator<FetchedMessage> messages) {
    int count = 0;
    while (messages.hasNext()) {
      FetchedMessage message = messages.next();
      try {
        GenericRecord genericRecord = serializer.toGenericRecord(message.getPayload());
        ILoggingEvent event = serializer.fromGenericRecord(genericRecord);
        LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(event.getMDCPropertyMap());

        synchronized (messageTable) {
          long key = event.getTimeStamp() / eventBucketIntervalMs;
          List<KafkaLogEvent> msgList = messageTable.get(key, loggingContext.getLogPathFragment());
          if (msgList == null) {
            msgList = Lists.newArrayList();
            messageTable.put(key, loggingContext.getLogPathFragment(), msgList);
          }
          msgList.add(new KafkaLogEvent(genericRecord, event, loggingContext,
                                        message.getTopicPartition().getPartition(), message.getNextOffset()));
        }
      } catch (Throwable e) {
        LOG.warn("Exception while processing message with nextOffset {}. Skipping it.", message.getNextOffset(), e);
      }
      ++count;
    }
    LOG.debug("Got {} messages from kafka", count);
  }

  @Override
  public void finished() {
    LOG.info("LogCollectorCallback finished.");
  }
}
