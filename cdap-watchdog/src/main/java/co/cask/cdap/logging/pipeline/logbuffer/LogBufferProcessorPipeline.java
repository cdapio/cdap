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

package co.cask.cdap.logging.pipeline.logbuffer;

import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.logging.logbuffer.ConcurrentLogBufferWriter;
import co.cask.cdap.logging.logbuffer.LogBufferEvent;
import co.cask.cdap.logging.logbuffer.LogBufferFileOffset;
import co.cask.cdap.logging.meta.Checkpoint;
import co.cask.cdap.logging.meta.CheckpointManager;
import co.cask.cdap.logging.pipeline.LogProcessorPipelineContext;
import co.cask.cdap.logging.pipeline.queue.ProcessedEventMetadata;
import co.cask.cdap.logging.pipeline.queue.ProcessorEvent;
import co.cask.cdap.logging.pipeline.queue.TimeEventQueueProcessor;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Log processing pipeline to process log events from log buffer. Events are pushed to this pipeline from
 * {@link ConcurrentLogBufferWriter}.
 */
public class LogBufferProcessorPipeline extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(LogBufferProcessorPipeline.class);
  // For outage, only log once per 60 seconds per message.
  private static final Logger OUTAGE_LOG =
    Loggers.sampling(LOG, LogSamplers.perMessage(() -> LogSamplers.limitRate(60000)));
  private static final int INCOMING_EVENT_QUEUE_SIZE = 10000;

  private final String name;
  private final int instanceId;
  private final LogBufferPipelineConfig config;
  private final LogProcessorPipelineContext context;
  private final CheckpointManager<LogBufferFileOffset> checkpointManager;
  private final MetricsContext metricsContext;
  private final TimeEventQueueProcessor<LogBufferFileOffset> eventQueueProcessor;
  private final BlockingQueue<LogBufferEvent> incomingEventQueue;
  private final Map<Integer, MutableLogBufferCheckpoint> checkpoints;
  private final CountDownLatch stopLatch;

  private volatile boolean stopped;
  private long lastCheckpointTime;
  private int unSyncedEvents;

  public LogBufferProcessorPipeline(LogProcessorPipelineContext context, LogBufferPipelineConfig config,
                                    CheckpointManager<LogBufferFileOffset> checkpointManager, int instanceId) {
    this.name = context.getName();
    this.instanceId = instanceId;
    this.config = config;
    this.context = context;
    this.checkpointManager = checkpointManager;
    this.metricsContext = context;
    this.eventQueueProcessor = new TimeEventQueueProcessor<>(context, config.getMaxBufferSize(),
                                                             config.getEventDelayMillis(), ImmutableSet.of(instanceId));
    this.incomingEventQueue = new ArrayBlockingQueue<>(INCOMING_EVENT_QUEUE_SIZE);
    this.checkpoints = new HashMap<>();
    this.stopLatch = new CountDownLatch(1);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting log processor pipeline for {} with configurations {}", name, config);
    Checkpoint<LogBufferFileOffset> checkpoint = checkpointManager.getCheckpoint(instanceId);

    checkpoints.put(0, new MutableLogBufferCheckpoint(checkpoint.getOffset().getFileId(),
                                                      checkpoint.getOffset().getFilePos(),
                                                      checkpoint.getMaxEventTime()));
    context.start();
    LOG.info("Log processor pipeline for {} with config {} started with checkpoint {}", name, config, this.checkpoints);
  }

  @Override
  protected void run() throws Exception {
    lastCheckpointTime = System.currentTimeMillis();
    while (!stopped) {
      boolean hasEventProcessed = processEvents(incomingEventQueue);
      long now = System.currentTimeMillis();
      long nextCheckpointDelay = trySyncAndPersistCheckpoints(now);

      // If nothing has been processed (e.g. fail to append anything to appender),
      // Wait until min(next checkpoint delay, next event delay).
      if (!hasEventProcessed) {
        long sleepMillis = config.getEventDelayMillis();
        sleepMillis = Math.min(sleepMillis, nextCheckpointDelay);
        if (sleepMillis > 0) {
          stopLatch.await(sleepMillis, TimeUnit.MILLISECONDS);
        }
      }
    }
  }

  @Override
  protected void triggerShutdown() {
    stopped = true;
    stopLatch.countDown();
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Shutting down log processor pipeline for {}", name);

    try {
      context.stop();
    } catch (Exception e) {
      // Just log, not to fail the shutdown
      LOG.warn("Exception raised when stopping pipeline {}", name, e);
    }

    persistCheckpoints();
    LOG.info("Log processor pipeline for {} stopped with latest checkpoint {}", name, checkpoints);
  }

  @Override
  protected String getServiceName() {
    return "LogPipeline-" + name;
  }

  /**
   * Reads events from incomingEventQueue and sends them to event processor for further processing.
   */
  private boolean processEvents(BlockingQueue<LogBufferEvent> incomingEventQueue) {
    ProcessedEventMetadata<LogBufferFileOffset> metadata
      = eventQueueProcessor.process(0, new LogFileOffsetTransformIterator(incomingEventQueue));

    // none of the events were processed.
    if (metadata.getTotalEventsProcessed() <= 0) {
      return false;
    }

    unSyncedEvents += metadata.getTotalEventsProcessed();
    // events were processed, so update the checkpoints
    Checkpoint<LogBufferFileOffset> checkpoint = metadata.getCheckpoints().get(instanceId);
    MutableLogBufferCheckpoint mutableCheckpoint = checkpoints.get(instanceId);
    MutableLogBufferFileOffset offset = mutableCheckpoint.getOffset();
    offset.setFileId(checkpoint.getOffset().getFileId());
    offset.setFilePos(checkpoint.getOffset().getFilePos());
    mutableCheckpoint.setMaxEventTs(checkpoint.getMaxEventTime());

    return true;
  }

  /**
   * Persists the checkpoints.
   */
  private void persistCheckpoints() {
    try {
      checkpointManager.saveCheckpoints(checkpoints);
      LOG.debug("Checkpoint persisted for {} with {}", name, checkpoints);
    } catch (IOException e) {
      // Just log as it is non-fatal if failed to save checkpoints. If the pipeline stops and checkpoints are not
      // persisted, there will be duplicate logs. Its ok to have duplicate logs in this error scenario. However, log
      // events must never be lost.
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
   * Pushes log events to blocking queue to be processed by processor pipeline.
   *
   * @param events log events to be processed
   */
  public void processLogEvents(Iterator<LogBufferEvent> events) {
    // Don't accept any log events if the pipeline is not running
    while (!stopped && events.hasNext()) {
      try {
        // This call will block caller thread until the queue has free space.
        incomingEventQueue.put(events.next());
      } catch (InterruptedException e) {
        // Just ignore the exception and reset the flag
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Returns smallest checkpoint offset for this pipeline.
   */
  public LogBufferFileOffset getSmallestCheckpointOffset() throws IOException {
    LogBufferFileOffset offset = checkpointManager.getCheckpoint(instanceId).getOffset();
    if (offset.getFileId() < 0) {
      return new LogBufferFileOffset(0, 0);
    }
    return offset;
  }

  /**
   * A mutable implementation of {@link Checkpoint}.
   */
  private static final class MutableLogBufferCheckpoint extends Checkpoint<LogBufferFileOffset> {
    private MutableLogBufferFileOffset offset;
    private long maxEventTs;

    MutableLogBufferCheckpoint(long fileId, long filePos, long maxEventTs) {
      super(new LogBufferFileOffset(fileId, filePos), maxEventTs);
      this.offset = new MutableLogBufferFileOffset(fileId, filePos);
      this.maxEventTs = maxEventTs;
    }

    @Override
    public MutableLogBufferFileOffset getOffset() {
      return offset;
    }

    @Override
    public long getMaxEventTime() {
      return maxEventTs;
    }

    void setMaxEventTs(long maxEventTs) {
      this.maxEventTs = maxEventTs;
    }

    @Override
    public String toString() {
      return "MutableLogBufferCheckpoint{" +
        "offset=" + offset +
        ", maxEventTs=" + maxEventTs +
        '}';
    }
  }

  /**
   * A mutable implementation of {@link LogBufferFileOffset}.
   */
  private static final class MutableLogBufferFileOffset extends LogBufferFileOffset {
    private long fileId;
    private long filePos;

    MutableLogBufferFileOffset(long fileId, long filePos) {
      super(fileId, filePos);
      this.fileId = fileId;
      this.filePos = filePos;
    }

    @Override
    public long getFileId() {
      return fileId;
    }

    @Override
    public long getFilePos() {
      return filePos;
    }

    void setFileId(long fileId) {
      this.fileId = fileId;
    }

    void setFilePos(long filePos) {
      this.filePos = filePos;
    }

    @Override
    public String toString() {
      return "MutableLogBufferFileOffset{" +
        "fileId=" + fileId +
        ", filePos=" + filePos +
        '}';
    }
  }

  /**
   * Iterator to transform LogBufferEvent to ProcessorEvent.
   */
  private final class LogFileOffsetTransformIterator implements Iterator<ProcessorEvent<LogBufferFileOffset>> {
    private final BlockingQueue<LogBufferEvent> queue;
    private int count = 0;

    LogFileOffsetTransformIterator(BlockingQueue<LogBufferEvent> queue) {
      this.queue = queue;
    }

    @Override
    public boolean hasNext() {
      // if the count has reached batch size or if there are not more elements in the queue at this moment, that
      // means no more events should be processed. So return false
      return count < config.getBatchSize() && queue.peek() != null;
    }

    @Override
    public ProcessorEvent<LogBufferFileOffset> next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      LogBufferEvent nextEvent = queue.poll();
      count++;
      return new ProcessorEvent<>(nextEvent.getLogEvent(), nextEvent.getEventSize(), nextEvent.getOffset());
    }
  }
}
