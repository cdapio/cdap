/*
 * Copyright © 2014 Cask Data, Inc.
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
import com.google.common.collect.RowSortedTable;
import org.apache.avro.generic.GenericRecord;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Kafka callback to fetch log messages and store them in time buckets per logging context.
 */
public class LogCollectorCallback implements KafkaConsumer.MessageCallback {
  private static final Logger LOG = LoggerFactory.getLogger(LogCollectorCallback.class);

  private final RowSortedTable<Long, String, Entry<Long, List<KafkaLogEvent>>> messageTable;
  private final LoggingEventSerializer serializer;
  private final long eventBucketIntervalMs;
  private final long maxNumberOfBucketsInTable;
  private final CountDownLatch kafkaCancelCallbackLatch;
  private static final long SLEEP_TIME_MS = 100;
  private final String logBaseDir;

  public LogCollectorCallback(RowSortedTable<Long, String, Entry<Long, List<KafkaLogEvent>>> messageTable,
                              LoggingEventSerializer serializer, long eventBucketIntervalMs,
                              long maxNumberOfBucketsInTable, CountDownLatch kafkaCancelCallbackLatch,
                              String logBaseDir) {
    this.messageTable = messageTable;
    this.serializer = serializer;
    this.eventBucketIntervalMs = eventBucketIntervalMs;
    this.maxNumberOfBucketsInTable = maxNumberOfBucketsInTable;
    this.kafkaCancelCallbackLatch = kafkaCancelCallbackLatch;
    this.logBaseDir = logBaseDir;
  }

  @Override
  public void onReceived(Iterator<FetchedMessage> messages) {

    try {
      if (kafkaCancelCallbackLatch.await(50, TimeUnit.MICROSECONDS)) {
        // if count down occurred return
        LOG.info("Returning since callback is cancelled.");
        return;
      }
    } catch (InterruptedException e) {
      LOG.error("Exception: ", e);
      Thread.currentThread().interrupt();
      return;
    }

    int count = 0;

    while (messages.hasNext()) {
      FetchedMessage message = messages.next();
      try {
        GenericRecord genericRecord = serializer.toGenericRecord(message.getPayload());
        ILoggingEvent event = serializer.fromGenericRecord(genericRecord);
        LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(event.getMDCPropertyMap());

        // Compute the bucket number for the current event
        long key = event.getTimeStamp() / eventBucketIntervalMs;

        while (true) {
          // Get the oldest bucket in the table
          long oldestBucketKey = 0;
          synchronized (messageTable) {
            SortedSet<Long> rowKeySet = messageTable.rowKeySet();
            if (rowKeySet.isEmpty()) {
              // Table is empty so go ahead and add the current event in the table
              break;
            }
            oldestBucketKey = rowKeySet.first();
          }

          // If the current event falls in the bucket number which is not in window [oldestBucketKey, oldestBucketKey+8]
          // sleep for the time duration till event falls in the window
          if (key > (oldestBucketKey + maxNumberOfBucketsInTable)) {
            LOG.debug("key={}, oldestBucketKey={}, maxNumberOfBucketsInTable={}. Sleeping for {} ms.",
                     key, oldestBucketKey, maxNumberOfBucketsInTable, SLEEP_TIME_MS);

            if (kafkaCancelCallbackLatch.await(SLEEP_TIME_MS, TimeUnit.MILLISECONDS)) {
              // if count down occurred return
              LOG.info("Returning since callback is cancelled");
              return;
            }
          } else {
            break;
          }
        }

        synchronized (messageTable) {
          Entry<Long, List<KafkaLogEvent>> entry = messageTable.get(key, loggingContext.getLogPathFragment(logBaseDir));
          List<KafkaLogEvent> msgList = null;
          if (entry == null) {
            long eventArrivalBucketKey = System.currentTimeMillis() / eventBucketIntervalMs;
            msgList = Lists.newArrayList();
            messageTable.put(key, loggingContext.getLogPathFragment(logBaseDir),
                             new AbstractMap.SimpleEntry<Long, List<KafkaLogEvent>>(eventArrivalBucketKey, msgList));
          } else {
           msgList = messageTable.get(key, loggingContext.getLogPathFragment(logBaseDir)).getValue();
          }
          msgList.add(new KafkaLogEvent(genericRecord, event, loggingContext,
                                        message.getTopicPartition().getPartition(), message.getNextOffset()));
        }
      } catch (Throwable e) {
        LOG.warn("Exception while processing message with nextOffset {}. Skipping it.", message.getNextOffset(), e);
      }
      ++count;
    }
    LOG.trace("Got {} messages from kafka", count);
    if (count > 0) {
      LOG.debug("Got {} messages from kafka", count);
    }
  }

  @Override
  public void finished() {
    LOG.info("LogCollectorCallback finished.");
  }
}
