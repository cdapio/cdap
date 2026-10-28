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

package io.cdap.cdap.logging.pipeline.queue;

import ch.qos.logback.classic.spi.ILoggingEvent;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.LogSamplers;
import io.cdap.cdap.common.logging.Loggers;
import io.cdap.cdap.logging.meta.Checkpoint;
import io.cdap.cdap.logging.pipeline.LogProcessorPipelineContext;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link TimeEventQueue} processor to enqueue the log events to {@link TimeEventQueue}, and
 * process them.
 *
 * @param <OFFSET> type of the offset
 */
public class TimeEventQueueProcessor<OFFSET extends Comparable<OFFSET>> {

  private static final Logger LOG = LoggerFactory.getLogger(TimeEventQueueProcessor.class);
  // For outage, only log once per 60 seconds per message.
  private static final Logger OUTAGE_LOG =
      Loggers.sampling(LOG, LogSamplers.perMessage(() -> LogSamplers.limitRate(60000)));
  // For dropped (unprocessable) events, log at most once per 60 seconds so that a burst of bad
  // records cannot flood the log while the drop itself stays visible.
  private static final Logger DROP_LOG = Loggers.sampling(LOG, LogSamplers.limitRate(60000));
  // Metric name for events that were dropped because they can never be processed.
  private static final String DROPPED_INVALID_METRIC = "event.dropped.invalid";
  private static final double MIN_FREE_FACTOR = 0.5d;
  private final TimeEventQueue<ILoggingEvent, OFFSET> eventQueue;
  private final LogProcessorPipelineContext context;
  private final MetricsContext metricsContext;
  private final long maxBufferSize;
  private final long eventDelayMillis;

  /**
   * Time event queue processor.
   */
  public TimeEventQueueProcessor(LogProcessorPipelineContext context, long maxBufferSize,
      long eventDelayMillis,
      Iterable<Integer> partitions) {
    this.context = context;
    this.maxBufferSize = maxBufferSize;
    this.eventDelayMillis = eventDelayMillis;
    this.metricsContext = context;
    this.eventQueue = new TimeEventQueue<>(partitions);
  }

  /**
   * Processes events provided by event iterator for a given partition.
   *
   * @param partition log event partition
   * @param eventIterator processor events iterator
   * @return processed event metadata
   */
  public ProcessedEventMetadata<OFFSET> process(int partition,
      Iterator<ProcessorEvent<OFFSET>> eventIterator) {
    int totalEvents = 0;
    Map<Integer, Checkpoint<OFFSET>> checkpoints = new HashMap<>();

    // iterate through all the events if buffer size has not reached max
    while (eventIterator.hasNext()) {
      if (eventQueue.getEventSize() >= maxBufferSize) {
        OUTAGE_LOG.info("Maximum queue size {} reached for log pipeline {}.", maxBufferSize,
            context.getName());

        // Event queue is full. So try to append events to log appenders. If none of the events are appended to log
        // appenders, then do not enqueue any more events.
        ProcessedEventMetadata<OFFSET> eventsMetadata = append();
        if (eventsMetadata.getTotalEventsProcessed() <= 0) {
          break;
        }
        totalEvents += eventsMetadata.getTotalEventsProcessed();
        checkpoints.putAll(eventsMetadata.getCheckpoints());
      }

      ProcessorEvent<OFFSET> processorEvent = eventIterator.next();
      eventQueue.add(processorEvent.getEvent(), processorEvent.getEvent().getTimeStamp(),
          processorEvent.getEventSize(),
          partition, processorEvent.getOffset());
    }

    // if event queue is full or all the events have been added to the queue, append all the enqueued events to log
    // appenders.
    ProcessedEventMetadata<OFFSET> eventsMetadata = append();
    if (eventsMetadata.getTotalEventsProcessed() > 0) {
      totalEvents += eventsMetadata.getTotalEventsProcessed();
      checkpoints.putAll(eventsMetadata.getCheckpoints());
    }

    return new ProcessedEventMetadata<>(totalEvents, checkpoints);
  }

  private ProcessedEventMetadata<OFFSET> append() {
    long minEventTime = System.currentTimeMillis() - eventDelayMillis;
    long maxRetainSize = eventQueue.getEventSize() >= maxBufferSize
        ? (long) (maxBufferSize * MIN_FREE_FACTOR) : Long.MAX_VALUE;

    int eventsAppended = 0;
    int eventsDropped = 0;
    long minDelay = Long.MAX_VALUE;
    long maxDelay = -1;
    Map<Integer, Checkpoint<OFFSET>> metadata = new HashMap<>();

    TimeEventQueue.EventIterator<ILoggingEvent, OFFSET> iterator = eventQueue.iterator();
    while (iterator.hasNext()) {
      ILoggingEvent event = iterator.next();

      // If not forced to reduce the event queue size and the current event timestamp is still within the
      // buffering time, no need to iterate anymore
      if (eventQueue.getEventSize() <= maxRetainSize && event.getTimeStamp() >= minEventTime) {
        break;
      }

      // update delay
      long delay = System.currentTimeMillis() - event.getTimeStamp();
      minDelay = delay < minDelay ? delay : minDelay;
      maxDelay = delay > maxDelay ? delay : maxDelay;

      String loggerName = event.getLoggerName();
      if (loggerName == null) {
        // A log event without a logger name cannot be routed to any appender. This happens when
        // a damaged record is read back from the log buffer (e.g. after a hard node restart).
        dropEvent(iterator, event, metadata, "Log event has no logger name", null);
        eventsDropped++;
        continue;
      }

      try {
        // Otherwise, append the event
        ch.qos.logback.classic.Logger effectiveLogger = context.getEffectiveLogger(loggerName);
        if (event.getLevel().isGreaterOrEqual(effectiveLogger.getEffectiveLevel())) {
          effectiveLogger.callAppenders(event);
        }
      } catch (IllegalArgumentException | NullPointerException e) {
        // Deterministic failure: retrying the same event can never succeed. Drop it and move on.
        // Breaking here instead would retry the event forever: the checkpoint never advances and
        // the queue grows until the JVM runs out of memory.
        dropEvent(iterator, event, metadata, "Unprocessable log event", e);
        eventsDropped++;
        continue;
      } catch (Exception e) {
        OUTAGE_LOG.warn("Failed to append log event in log pipeline {}. Will be retried.",
            context.getName(), e);
        break;
      }

      checkpointAndRemove(iterator, event, metadata);
      eventsAppended++;
    }

    // Always try to call flush, even there was no event written. This is needed so that appender get called
    // periodically even there is no new events being appended to perform housekeeping work.
    // Failure to flush is ok and it will be retried by the wrapped appender
    try {
      context.flush();
    } catch (IOException e) {
      LOG.warn("Failed to flush in pipeline {}. Will be retried.", context.getName(), e);
    }
    metricsContext.gauge("event.queue.size.bytes", eventQueue.getEventSize());

    // If no event was appended or dropped, just return with 0 events processed.
    if (eventsAppended + eventsDropped == 0) {
      // nothing has been appended so there is no metadata associated with those events. So return null.
      return new ProcessedEventMetadata<>(0, null);
    }

    metricsContext.gauge(Constants.Metrics.Name.Log.PROCESS_MIN_DELAY, minDelay);
    metricsContext.gauge(Constants.Metrics.Name.Log.PROCESS_MAX_DELAY, maxDelay);
    metricsContext.increment(Constants.Metrics.Name.Log.PROCESS_MESSAGES_COUNT, eventsAppended);

    // Dropped events count as processed so that the checkpoint moves past them and gets persisted.
    return new ProcessedEventMetadata<>(eventsAppended + eventsDropped, metadata);
  }

  /**
   * Drops an event that can never be delivered to any appender: logs it (rate limited), emits the
   * {@code event.dropped.invalid} metric and advances the checkpoint past it.
   *
   * @param iterator the queue iterator positioned at the event to drop
   * @param event the event to drop
   * @param metadata the checkpoint map to update
   * @param reason human readable reason for the drop
   * @param cause the exception that made the event unprocessable, or {@code null} if none
   */
  private void dropEvent(TimeEventQueue.EventIterator<ILoggingEvent, OFFSET> iterator,
      ILoggingEvent event, Map<Integer, Checkpoint<OFFSET>> metadata,
      String reason, @Nullable Exception cause) {
    DROP_LOG.warn("Dropping log event in log pipeline {} at offset {}: {}",
        context.getName(), iterator.getOffset(), reason, cause);
    metricsContext.increment(DROPPED_INVALID_METRIC, 1);
    checkpointAndRemove(iterator, event, metadata);
  }

  /**
   * Records the checkpoint for the partition of the current event and removes the event from the
   * queue.
   */
  private void checkpointAndRemove(TimeEventQueue.EventIterator<ILoggingEvent, OFFSET> iterator,
      ILoggingEvent event, Map<Integer, Checkpoint<OFFSET>> metadata) {
    metadata.put(iterator.getPartition(),
        new Checkpoint<>(eventQueue.getSmallestOffset(iterator.getPartition()),
            event.getTimeStamp()));
    iterator.remove();
  }

  /**
   * Returns if the queue is empty for a given partition.
   */
  public boolean isQueueEmpty(int partition) {
    return eventQueue.isEmpty(partition);
  }
}
