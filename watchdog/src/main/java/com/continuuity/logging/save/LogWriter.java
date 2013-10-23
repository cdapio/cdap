package com.continuuity.logging.save;

import com.continuuity.logging.kafka.KafkaLogEvent;
import com.continuuity.logging.write.LogFileWriter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Persists bucketized logs stored by {@link LogCollectorCallback}.
 */
public class LogWriter implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(LogWriter.class);
  private final LogFileWriter<KafkaLogEvent> logFileWriter;
  private final Table<Long, String, List<KafkaLogEvent>> messageTable;
  private final long eventProcessingDelayMs;
  private final long eventBucketIntervalMs;

  private final ListMultimap<String, KafkaLogEvent> writeListMap = ArrayListMultimap.create();
  private int messages = 0;

  public LogWriter(LogFileWriter<KafkaLogEvent> logFileWriter, Table<Long, String, List<KafkaLogEvent>> messageTable,
                   long eventProcessingDelayMs, long eventBucketIntervalMs) {
    this.logFileWriter = logFileWriter;
    this.messageTable = messageTable;
    this.eventProcessingDelayMs = eventProcessingDelayMs;
    this.eventBucketIntervalMs = eventBucketIntervalMs;
  }

  @Override
  public void run() {
    try {
      // Read new messages only if previous write was successful.
      if (writeListMap.isEmpty()) {
        messages = 0;
        long processKey = (System.currentTimeMillis() - eventProcessingDelayMs) / eventBucketIntervalMs;
        synchronized (messageTable) {
          for (Iterator<Table.Cell<Long, String, List<KafkaLogEvent>>> it = messageTable.cellSet().iterator();
               it.hasNext(); ) {
            Table.Cell<Long, String, List<KafkaLogEvent>> cell = it.next();
            // Process only messages older than eventProcessingDelayMs
            if (cell.getRowKey() >= processKey) {
              continue;
            }

            writeListMap.putAll(cell.getColumnKey(), cell.getValue());
            messages += cell.getValue().size();
            it.remove();
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
    } catch (Throwable e) {
      LOG.error("Caught exception during save, will try again.", e);
    }
  }
}
