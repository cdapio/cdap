/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.logging.pipeline.buffer;

import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.logging.buffer.BufferLogEvent;
import co.cask.cdap.logging.buffer.BufferReader;
import co.cask.cdap.logging.buffer.FileOffset;
import co.cask.cdap.logging.meta.CheckpointManager;
import co.cask.cdap.logging.pipeline.LogPipelineConfig;
import co.cask.cdap.logging.pipeline.LogProcessorPipelineContext;
import co.cask.cdap.logging.pipeline.queue.AppendedEventMetadata;
import co.cask.cdap.logging.pipeline.queue.CheckpointMetadata;
import co.cask.cdap.logging.pipeline.queue.TimeEventQueueProcessor;
import co.cask.cdap.logging.serialize.LoggingEventSerializer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Log processor pipeline to read from log buffer and append to all the appenders. It also maintains the checkpoints
 * to which it has appended log messages successfully.
 */
public class BufferLogProcessorPipeline extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(BufferLogProcessorPipeline.class);
  // For outage, only log once per 60 seconds per message.
  private static final Logger OUTAGE_LOG =
    Loggers.sampling(LOG, LogSamplers.perMessage(() -> LogSamplers.limitRate(60000)));
  private static final int BATCH_SIZE = 100;

  private final String name;
  private final LogProcessorPipelineContext context;
  private final LogPipelineConfig config;
  private final CheckpointManager<BufferCheckpoint> checkpointManager;
  private final MetricsContext metricsContext;
  private final TimeEventQueueProcessor<FileOffset> eventQueueProcessor;
  private final BufferReader reader;
  private final LoggingEventSerializer serializer;
  private MutableBufferCheckpoint checkpoint;
  private FileOffset offset;

  private volatile Thread runThread;
  private volatile boolean stopped;
  private long lastCheckpointTime;
  private int unSyncedEvents;

  public BufferLogProcessorPipeline(LogProcessorPipelineContext context,
                                    CheckpointManager<BufferCheckpoint> checkpointManager, BufferReader reader) {
    this.name = context.getName();
    this.context = context;
    // TODO Emit correct metrics
    this.metricsContext = context;
    this.checkpointManager = checkpointManager;
    // TODO Figure out details for buffer pipeline conf
    this.config = new LogPipelineConfig(1, 2, 3);
    this.eventQueueProcessor = new TimeEventQueueProcessor<>(name, context, config, ImmutableSet.of(0));
    this.reader = reader;
    this.serializer = new LoggingEventSerializer();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting log processor pipeline for {} with configurations {}", name, config);
    // Reads the offsets from checkpoint
    if (offset == null) {
      offset = checkpoint.getFilePos() < 0 && checkpoint.getMaxEventTs() < 0 ? null :
        new FileOffset(checkpoint.getFileId(), checkpoint.getMaxEventTs());
    }
    context.start();
    LOG.info("Log processor pipeline for {} with config {} started with checkpoint {}", name, config, checkpoint);
  }

  @Override
  protected void run() throws Exception {
    runThread = Thread.currentThread();

    try {
      lastCheckpointTime = System.currentTimeMillis();
      while (!stopped) {
        List<BufferLogEvent> events = reader.read(offset, BATCH_SIZE);
        boolean hasMessageProcessed = processMessages(events);

        long now = System.currentTimeMillis();
        long nextCheckpointDelay = trySyncAndPersistCheckpoints(now);

        // If nothing has been processed (e.g. fail to append anything to appender),
        // Sleep until min(next checkpoint delay, next event delay).
        if (!hasMessageProcessed) {
          long sleepMillis = config.getEventDelayMillis();
          sleepMillis = Math.min(sleepMillis, nextCheckpointDelay);
          if (sleepMillis > 0) {
            TimeUnit.MILLISECONDS.sleep(sleepMillis);
          }
        }
      }
    } catch (InterruptedException e) {
      // Interruption means stopping the service.
    }
  }

  @Override
  protected void triggerShutdown() {
    stopped = true;
    if (runThread != null) {
      runThread.interrupt();
    }
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Shutting down log processor pipeline for {}", name);

    try {
      context.stop();
      // Persist the checkpoints. It can only be done after successfully stopping the appenders.
      // Since persistCheckpoint never throw, putting it inside try is ok.
      persistCheckpoints();
    } catch (Exception e) {
      // Just log, not to fail the shutdown
      LOG.warn("Exception raised when stopping pipeline {}", name, e);
    }

    LOG.info("Log processor pipeline for {} stopped with latest checkpoint {}", name, checkpoint);
  }

  @Override
  protected String getServiceName() {
    return "LogPipeline-" + name;
  }

  /**
   * Enqueue provided log events to time event queue.
   */
  private boolean processMessages(List<BufferLogEvent> logEvents) {
    boolean processed = false;
    for (BufferLogEvent logEvent : logEvents) {
      AppendedEventMetadata<FileOffset> metadata = eventQueueProcessor
        .appendAndEnqueue(logEvent.getLogEvent(), serializer.toBytes(logEvent.getLogEvent()).length,
                          0, logEvent.getOffset());

      // If queue is full, do not enqueue events.
      if (metadata.getEventsAppended() < 0) {
        break;
      }

      processed = true;

      // nothing was appended so just continue
      if (metadata.getEventsAppended() == 0) {
        continue;
      }

      unSyncedEvents += metadata.getEventsAppended();
      // update offset and checkpoint
      Map<Integer, CheckpointMetadata<FileOffset>> checkpointMetadata = metadata.getCheckpointMetadata();
      FileOffset smallestOffset = checkpointMetadata.get(0).getNextOffset();
      if (checkpoint == null) {
        checkpoint = new MutableBufferCheckpoint(smallestOffset.getFileId(), smallestOffset.getFilePos(),
                                                 checkpointMetadata.get(0).getMaxEventTime());
      } else {
        checkpoint.setFileId(smallestOffset.getFileId())
          .setFilePos(smallestOffset.getFilePos())
          .setMaxEventTs(checkpointMetadata.get(0).getMaxEventTime());
      }

      // keep track of what was enqueued till now. This is to keep track of number of events read so far.
      offset = logEvent.getOffset();
    }

    return processed;
  }

  // ==== Checkpoint management (same for both pipelines)  ====

  /**
   * Persists the checkpoints.
   */
  private void persistCheckpoints() {
    try {
      checkpointManager.saveCheckpoints(ImmutableMap.of(0, checkpoint));
      LOG.debug("Checkpoint persisted for {} with {}", name, checkpoint);
    } catch (Exception e) {
      // Just log as it is non-fatal if failed to save checkpoints
      OUTAGE_LOG.warn("Failed to persist checkpoint for pipeline {}.", name, e);
    }
  }

  /**
   * Sync the appender and persists checkpoints if it is time.
   *
   * @return delay in millisecond till the next sync time.
   */
  private long trySyncAndPersistCheckpoints(long currentTimeMillis) {
    if (unSyncedEvents == 0) {
      return config.getCheckpointIntervalMillis();
    }
    if (currentTimeMillis - config.getCheckpointIntervalMillis() < lastCheckpointTime) {
      return config.getCheckpointIntervalMillis() - currentTimeMillis + lastCheckpointTime;
    }

    // Sync the appender and persists checkpoints
    try {
      context.sync();
      // Only persist if sync succeeded. Since persistCheckpoints never throw, it's ok to be inside the try.
      persistCheckpoints();
      lastCheckpointTime = currentTimeMillis;
      metricsContext.gauge("last.checkpoint.time", lastCheckpointTime);
      unSyncedEvents = 0;
      LOG.debug("Events synced and checkpoint persisted for {}", name);
    } catch (Exception e) {
      OUTAGE_LOG.warn("Failed to sync in pipeline {}. Will be retried.", name, e);
    }
    return config.getCheckpointIntervalMillis();
  }

  /**
   * Mutable checkpoint for buffer log processor.
   */
  private static final class MutableBufferCheckpoint extends BufferCheckpoint {
    private String fileId;
    private long filePos;
    private long maxEventTs;

    MutableBufferCheckpoint(String fileId, long filePos, long maxEventTs) {
      super(fileId, filePos, maxEventTs);
      this.fileId = fileId;
      this.filePos = filePos;
      this.maxEventTs = maxEventTs;
    }

    @Override
    public String getFileId() {
      return fileId;
    }

    @Override
    public long getFilePos() {
      return filePos;
    }

    @Override
    public long getMaxEventTs() {
      return maxEventTs;
    }

    MutableBufferCheckpoint setFileId(String fileId) {
      this.fileId = fileId;
      return this;
    }

    MutableBufferCheckpoint setFilePos(long filePos) {
      this.filePos = filePos;
      return this;
    }

    MutableBufferCheckpoint setMaxEventTs(long maxEventTs) {
      this.maxEventTs = maxEventTs;
      return this;
    }

    @Override
    public String toString() {
      return "MutableBufferCheckpoint{" +
        "fileId='" + fileId + '\'' +
        ", filePos=" + filePos +
        ", maxEventTs=" + maxEventTs +
        '}';
    }
  }
}
