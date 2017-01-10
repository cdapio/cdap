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

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import co.cask.cdap.logging.kafka.KafkaLogEvent;
import co.cask.cdap.logging.write.LogFileWriter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.RowSortedTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Persists bucketized logs stored by {@link KafkaMessageCallback}.
 */
public class LogWriter implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(LogWriter.class);
  private static final long SLEEP_TIME_NS = TimeUnit.MILLISECONDS.toNanos(100);

  private final LogFileWriter<KafkaLogEvent> logFileWriter;
  private final RowSortedTable<Long, String, Entry<Long, List<KafkaLogEvent>>> messageTable;
  private final long eventBucketIntervalMs;
  private final long maxNumberOfBucketsInTable;
  private final CountDownLatch stopLatch;
  private final ExponentialBackoff exponentialBackoff;

  private final ListMultimap<String, KafkaLogEvent> writeListMap = ArrayListMultimap.create();

  public LogWriter(LogFileWriter<KafkaLogEvent> logFileWriter,
                   RowSortedTable<Long, String, Entry<Long, List<KafkaLogEvent>>> messageTable,
                   long eventBucketIntervalMs, long maxNumberOfBucketsInTable, final CountDownLatch stopLatch) {
    this.logFileWriter = logFileWriter;
    this.messageTable = messageTable;
    this.eventBucketIntervalMs = eventBucketIntervalMs;
    this.maxNumberOfBucketsInTable = maxNumberOfBucketsInTable;
    this.stopLatch = stopLatch;
    this.exponentialBackoff =
      new ExponentialBackoff(1, 60,
                             new ExponentialBackoff.BackoffHandler() {
                               @Override
                               public void handle(long backoff) throws InterruptedException {
                                 // Use stop latch for waiting so that we can exit immediately
                                 // when stopped.
                                 stopLatch.await(backoff, TimeUnit.SECONDS);
                               }
                             });
  }

  @Override
  public void run() {
    while (true) {
      try {
        // Read new messages only if previous write was successful.
        if (writeListMap.isEmpty()) {
          int messages = 0;

          // The newest event that we can write to disk
          // We try to buffer events up to (eventBucketIntervalMs * maxNumberOfBucketsInTable) time
          // so that we collect almost all events for a time bucket before we sort it.
          long limitKey = (System.currentTimeMillis() / eventBucketIntervalMs) - maxNumberOfBucketsInTable;
          synchronized (messageTable) {
            SortedSet<Long> rowKeySet = messageTable.rowKeySet();
            if (!rowKeySet.isEmpty()) {
              int numBuckets = rowKeySet.size();
              long oldestBucketKey = rowKeySet.first();

              Map<String, Entry<Long, List<KafkaLogEvent>>> row = messageTable.row(oldestBucketKey);
              for (Iterator<Map.Entry<String, Entry<Long, List<KafkaLogEvent>>>> it = row.entrySet().iterator();
                   it.hasNext(); ) {
                Map.Entry<String, Entry<Long, List<KafkaLogEvent>>> mapEntry = it.next();
                // Stop if event arrival time is more than the limit (this is for events being generated now)
                // However, if we have reached maxNumberOfBucketsInTable then it means we are reading old
                // events and we can write as soon as we fill up maxNumberOfBucketsInTable
                if (numBuckets < maxNumberOfBucketsInTable &&
                  limitKey < mapEntry.getValue().getKey()) {
                  break;
                }
                writeListMap.putAll(mapEntry.getKey(), mapEntry.getValue().getValue());
                messages += mapEntry.getValue().getValue().size();
                it.remove();
              }
            }
          }

          LOG.trace("Got {} log messages to save", messages);
        }

        long sleepTimeNanos = writeListMap.isEmpty() ? SLEEP_TIME_NS : 1;

        // Wait for more data to arrive if writeListMap is empty, otherwise check if stopped
        if (stopLatch.await(sleepTimeNanos, TimeUnit.NANOSECONDS)) {
          // if count down occurred return
          LOG.debug("Returning since stop latch is cancelled");
          return;
        } else {
          LOG.trace("Waiting for events, sleeping for {} ns", sleepTimeNanos);
        }


        for (Iterator<Entry<String, Collection<KafkaLogEvent>>> it = writeListMap.asMap().entrySet().iterator();
             it.hasNext(); ) {
          Entry<String, Collection<KafkaLogEvent>> mapEntry = it.next();
          List<KafkaLogEvent> list = (List<KafkaLogEvent>) mapEntry.getValue();

          LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
          List<ch.qos.logback.classic.Logger> loggerList = context.getLoggerList();
          for (ch.qos.logback.classic.Logger logger : loggerList) {
            if (logger.getName().equalsIgnoreCase("co.cask.cdap")) {
              logger.callAppenders(list.get(0).getLogEvent());
            }
          }

          if (!list.isEmpty()) {
            KafkaLogEvent kafkaLogEvent = list.get(0);
            ILoggingEvent logEvent = kafkaLogEvent.getLogEvent();
            LOG.info("Logger name: {}", logEvent.getLoggerName());
          }
          Collections.sort(list);
          logFileWriter.append(list);
          // Remove successfully written message
          it.remove();
        }

        logFileWriter.flush(false);

        // Reset backoff after a successful save
        exponentialBackoff.reset();
      } catch (Throwable e) {
        LOG.error("Caught exception during save, will try again with backoff.", e);
        try {
          exponentialBackoff.backoff();
        } catch (InterruptedException e1) {
          // Okay to ignore since we'll check stop latch in the next run, and exit if stopped
        }
      }
    }
  }
}
