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

package co.cask.cdap.logging.save;

import co.cask.cdap.logging.kafka.KafkaLogEvent;
import co.cask.cdap.logging.meta.Checkpoint;
import co.cask.cdap.logging.meta.CheckpointManager;
import co.cask.cdap.logging.write.AvroFileWriter;
import co.cask.cdap.logging.write.LogFileWriter;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * LogFileWriter that checkpoints kafka offsets for each partition.
 */
public class CheckpointingLogFileWriter implements LogFileWriter<KafkaLogEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(CheckpointingLogFileWriter.class);

  private final AvroFileWriter avroFileWriter;
  private final CheckpointManager checkpointManager;
  private final long flushIntervalMs;

  private long lastCheckpointTime = System.currentTimeMillis();
  private final Map<Integer, Checkpoint> partitionCheckpointMap = Maps.newHashMap();

  private final AtomicBoolean closed = new AtomicBoolean(false);

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
    Checkpoint maxCheckpoint = partitionCheckpointMap.get(partition);
    maxCheckpoint = maxCheckpoint == null ? new Checkpoint(-1, -1) : maxCheckpoint;

    for (KafkaLogEvent e : events) {
      if (e.getNextOffset() > maxCheckpoint.getNextOffset()) {
        maxCheckpoint = new Checkpoint(e.getNextOffset(), e.getLogEvent().getTimeStamp());
      }
    }

    partitionCheckpointMap.put(partition, maxCheckpoint);

    avroFileWriter.append(events);
  }

  @Override
  public void close() throws IOException {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    flush();
    avroFileWriter.close();
  }

  @Override
  public void flush() throws IOException {
    flush(true);
  }

  @Override
  public void flush(boolean force) throws IOException {
    try {
      long currentTs = System.currentTimeMillis();
      if (!force && currentTs - lastCheckpointTime < flushIntervalMs) {
        return;
      }

      avroFileWriter.flush();

      // Save the max checkpoint seen for each partition
      checkpointManager.saveCheckpoints(partitionCheckpointMap);
      lastCheckpointTime = currentTs;
    } catch (Exception e) {
      LOG.error("Got exception: ", e);
      throw new IOException(e);
    }
  }
}
