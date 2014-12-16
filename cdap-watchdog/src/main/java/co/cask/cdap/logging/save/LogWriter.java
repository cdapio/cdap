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

/**
 * Persists bucketized logs stored by {@link LogCollectorCallback}.
 */
public class LogWriter implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(LogWriter.class);
  private final LogFileWriter<KafkaLogEvent> logFileWriter;
  private final RowSortedTable<Long, String, Entry<Long, List<KafkaLogEvent>>> messageTable;
  private final long eventBucketIntervalMs;
  private final long maxNumberOfBucketsInTable;

  private final ListMultimap<String, KafkaLogEvent> writeListMap = ArrayListMultimap.create();
  private int messages = 0;

  public LogWriter(LogFileWriter<KafkaLogEvent> logFileWriter,
                   RowSortedTable<Long, String, Entry<Long, List<KafkaLogEvent>>> messageTable,
                   long eventBucketIntervalMs, long maxNumberOfBucketsInTable) {
    this.logFileWriter = logFileWriter;
    this.messageTable = messageTable;
    this.eventBucketIntervalMs = eventBucketIntervalMs;
    this.maxNumberOfBucketsInTable = maxNumberOfBucketsInTable;
  }

  @Override
  public void run() {
    try {
      // Read new messages only if previous write was successful.
      if (writeListMap.isEmpty()) {
        messages = 0;

        long limitKey = System.currentTimeMillis() / eventBucketIntervalMs;
        synchronized (messageTable) {
          SortedSet<Long> rowKeySet = messageTable.rowKeySet();
          if (!rowKeySet.isEmpty()) {
            // Get the oldest bucket in the table
            long oldestBucketKey = rowKeySet.first();

            Map<String, Entry<Long, List<KafkaLogEvent>>> row = messageTable.row(oldestBucketKey);
            for (Iterator<Map.Entry<String, Entry<Long, List<KafkaLogEvent>>>> it = row.entrySet().iterator();
                   it.hasNext(); ) {
              Map.Entry<String, Entry<Long, List<KafkaLogEvent>>> mapEntry = it.next();
              if (limitKey < (mapEntry.getValue().getKey() + maxNumberOfBucketsInTable)) {
                break;
              }
              writeListMap.putAll(mapEntry.getKey(), mapEntry.getValue().getValue());
              messages += mapEntry.getValue().getValue().size();
              it.remove();
            }
          }
        }

        LOG.debug("Got {} log messages to save", messages);

        for (Iterator<Map.Entry<String, Collection<KafkaLogEvent>>> it = writeListMap.asMap().entrySet().iterator();
             it.hasNext(); ) {
          Map.Entry<String, Collection<KafkaLogEvent>> mapEntry = it.next();
          List<KafkaLogEvent> list = (List<KafkaLogEvent>) mapEntry.getValue();
          Collections.sort(list);
          logFileWriter.append(list);
          // Remove successfully written message
          it.remove();
        }
      }
    } catch (Throwable e) {
      LOG.error("Caught exception during save, will try again.", e);
    }
  }
}
