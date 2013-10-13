package com.continuuity.logging.save;

import com.continuuity.logging.kafka.KafkaLogEvent;
import com.continuuity.logging.write.AvroFileWriter;
import com.continuuity.logging.write.LogFileWriter;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * LogFileWriter that checkpoints kafka offsets for each partition.
 */
public class CheckpointingLogFileWriter implements LogFileWriter<KafkaLogEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(CheckpointingLogFileWriter.class);

  private final AvroFileWriter avroFileWriter;
  private final CheckpointManager checkpointManager;
  private final long flushIntervalMs;

  private long lastCheckpointTime = System.currentTimeMillis();
  private Map<Integer, Long> partitionOffsetMap = Maps.newHashMap();

  public CheckpointingLogFileWriter(AvroFileWriter avroFileWriter, CheckpointManager checkpointManager,
                                    long flushIntervalMs) {
    this.avroFileWriter = avroFileWriter;
    this.checkpointManager = checkpointManager;
    this.flushIntervalMs = flushIntervalMs;
  }

  @Override
  public void append(List<KafkaLogEvent> events) throws Exception {
    if (events.isEmpty()) {
      return;
    }

    KafkaLogEvent event = events.get(0);
    int partition = event.getPartition();
    Long currentMaxOffset = partitionOffsetMap.get(partition);
    currentMaxOffset = currentMaxOffset == null ? -1 : currentMaxOffset;

    for (KafkaLogEvent e : events) {
      if (e.getNextOffset() > currentMaxOffset) {
        currentMaxOffset = e.getNextOffset();
      }
    }

    partitionOffsetMap.put(partition, currentMaxOffset);

    avroFileWriter.append(events);
    flush(false);
  }

  @Override
  public void close() throws IOException {
    flush();
    avroFileWriter.close();
  }

  @Override
  public void flush() throws IOException {
    try {
      flush(true);
    } catch (Exception e) {
      LOG.error("Got exception: ", e);
      throw new IOException(e);
    }
  }

  private void flush(boolean force) throws Exception {
    long currentTs = System.currentTimeMillis();
    if (!force && currentTs - lastCheckpointTime < flushIntervalMs) {
      return;
    }

    avroFileWriter.flush();

    // Save the max checkpoint seen for each partition
    for (Map.Entry<Integer, Long> entry : partitionOffsetMap.entrySet()) {
      LOG.debug("Saving checkpoint offset {} for partition {}", entry.getValue(), entry.getKey());
      checkpointManager.saveCheckpoint(entry.getKey(), entry.getValue());
    }
    lastCheckpointTime = currentTs;
  }
}
