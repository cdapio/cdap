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
import com.google.common.collect.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Persists bucketized logs stored by {@link LogCollectorCallback}.
 */
public class LogWriter implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(LogWriter.class);
  private final LogFileWriter<KafkaLogEvent> logFileWriter;
  private final RowSortedTable<Long, String, List<KafkaLogEvent>> messageTable;
  private final long eventBucketIntervalMs;
  private final long maxNumberOfBucketsInTable;

  private final ListMultimap<String, KafkaLogEvent> writeListMap = ArrayListMultimap.create();
  private int messages = 0;

  public LogWriter(LogFileWriter<KafkaLogEvent> logFileWriter,
                   RowSortedTable<Long, String, List<KafkaLogEvent>> messageTable,
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

        long currentBucketKey = System.currentTimeMillis() / eventBucketIntervalMs;
        synchronized (messageTable) {
          if (!messageTable.rowKeySet().isEmpty()) {
            // Get the oldest bucket in the table
            long oldestBucketKey = messageTable.rowKeySet().first();
            if (currentBucketKey > (oldestBucketKey + maxNumberOfBucketsInTable)) {
              Map<String, List<KafkaLogEvent>> row = messageTable.row(oldestBucketKey);

              for (Iterator<String> it = row.keySet().iterator(); it.hasNext(); ) {
                String key = it.next();
                writeListMap.putAll(key, row.get(key));
                messages += row.get(key).size();
                it.remove();
              }
            }
          }
        }

        LOG.debug("Got {} log messages to save", messages);

        for (Iterator<String> it = writeListMap.keySet().iterator(); it.hasNext(); ) {
          String key = it.next();
          List<KafkaLogEvent> list = writeListMap.get(key);
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
