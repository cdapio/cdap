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

package co.cask.cdap.logging.read;

import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.logging.appender.kafka.KafkaTopic;
import co.cask.cdap.logging.appender.kafka.StringPartitioner;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.save.CheckpointManager;
import co.cask.cdap.logging.save.CheckpointManagerFactory;
import co.cask.cdap.logging.save.KafkaLogWriterPlugin;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads logs in a distributed setup, using kafka for latest logs and files for older logs.
 */
public final class DistributedLogReader implements LogReader {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedLogReader.class);

  private final KafkaLogReader kafkaLogReader;
  private final FileLogReader fileLogReader;
  private final CheckpointManager checkpointManager;
  private final StringPartitioner partitioner;

  /**
   * Creates a DistributedLogReader object.
   */
  @Inject
  public DistributedLogReader(KafkaLogReader kafkaLogReader, FileLogReader fileLogReader,
                              CheckpointManagerFactory checkpointManagerFactory, StringPartitioner partitioner) {
    this.kafkaLogReader = kafkaLogReader;
    this.fileLogReader = fileLogReader;
    this.checkpointManager = checkpointManagerFactory.create(KafkaTopic.getTopic(),
                                                             KafkaLogWriterPlugin.CHECKPOINT_ROW_KEY_PREFIX);
    this.partitioner = partitioner;
  }

  @Override
  public void getLogNext(final LoggingContext loggingContext, final ReadRange readRange, final int maxEvents,
                              final Filter filter, final Callback callback) {
    // If latest logs are not requested, try reading from file.
    if (readRange != ReadRange.LATEST) {
      long checkpointTime = getCheckpointTime(loggingContext);
      // Read from file only if logs are saved for the loggingContext until fromTime
      if (readRange.getFromMillis() < checkpointTime) {
        fileLogReader.getLogNext(loggingContext, readRange, maxEvents, filter, callback);
        // If there are events from fileLogReader, return. Otherwise try in kafkaLogReader.
        if (callback.getCount() != 0) {
          return;
        }
      }
    }

    kafkaLogReader.getLogNext(loggingContext, readRange, maxEvents, filter, callback);
  }

  @Override
  public void getLogPrev(final LoggingContext loggingContext, final ReadRange readRange, final int maxEvents,
                              final Filter filter, final Callback callback) {
    // If latest logs are not requested, try reading from file.
    if (readRange != ReadRange.LATEST) {
      long checkpointTime = getCheckpointTime(loggingContext);
      // Read from file only if logs are saved for the loggingContext until fromTime
      if (readRange.getToMillis() < checkpointTime) {
        fileLogReader.getLogPrev(loggingContext, readRange, maxEvents, filter, callback);
        return;
      }
    }

    kafkaLogReader.getLogPrev(loggingContext, readRange, maxEvents, filter, callback);
  }

  @Override
  public void getLog(final LoggingContext loggingContext, final long fromTimeMs, final long toTimeMs,
                          final Filter filter, final Callback callback) {
    fileLogReader.getLog(loggingContext, fromTimeMs, toTimeMs, filter, callback);
  }

  private long getCheckpointTime(LoggingContext loggingContext) {
    int partition = partitioner.partition(loggingContext.getLogPartition(), -1);
    try {
      return checkpointManager.getCheckpoint(partition).getMaxEventTime();
    } catch (Exception e) {
      LOG.error("Got exception while reading checkpoint", e);
    }
    return -1;
  }
}
