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

package co.cask.cdap.logging.pipeline.queue;

import ch.qos.logback.classic.spi.ILoggingEvent;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.logging.meta.Checkpoint;
import co.cask.cdap.logging.pipeline.LogProcessorPipelineContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * The {@link TimeEventQueue} processor to enqueue the log events to {@link TimeEventQueue}, and process them.
 * @param <OFFSET> type of the offset
 */
public class TimeEventQueueProcessor<OFFSET extends Comparable<OFFSET>> {
  private static final Logger LOG = LoggerFactory.getLogger(TimeEventQueueProcessor.class);
  // For outage, only log once per 60 seconds per message.
  private static final Logger OUTAGE_LOG =
    Loggers.sampling(LOG, LogSamplers.perMessage(() -> LogSamplers.limitRate(60000)));
  private static final double MIN_FREE_FACTOR = 0.5d;
  private final TimeEventQueue<ILoggingEvent, OFFSET> eventQueue;
  private final LogProcessorPipelineContext context;
  private final MetricsContext metricsContext;
  private final long maxBufferSize;
  private final long eventDelayMillis;

  /**
   * Time event queue processor.
   */
  public TimeEventQueueProcessor(LogProcessorPipelineContext context, long maxBufferSize, long eventDelayMillis,
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
   *
   * @return processed event metadata
   */
  public ProcessedEventMetadata<OFFSET> process(int partition, Iterator<ProcessorEvent<OFFSET>> eventIterator) {
    int totalEvents = 0;
    Map<Integer, Checkpoint<OFFSET>> checkpoints = new HashMap<>();

    // iterate through all the events if buffer size has not reached max
    while (eventIterator.hasNext()) {
      if (eventQueue.getEventSize() >= maxBufferSize) {
        OUTAGE_LOG.info("Maximum queue size {} reached for log pipeline {}.", maxBufferSize, context.getName());

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
      eventQueue.add(processorEvent.getEvent(), processorEvent.getEvent().getTimeStamp(), processorEvent.getEventSize(),
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
    long maxRetainSize = eventQueue.getEventSize() >= maxBufferSize ?
      (long) (maxBufferSize * MIN_FREE_FACTOR) : Long.MAX_VALUE;

    int eventsAppended = 0;
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

      try {
        // Otherwise, append the event
        ch.qos.logback.classic.Logger effectiveLogger = context.getEffectiveLogger(event.getLoggerName());
        if (event.getLevel().isGreaterOrEqual(effectiveLogger.getEffectiveLevel())) {
          effectiveLogger.callAppenders(event);
        }
      } catch (Exception e) {
        OUTAGE_LOG.warn("Failed to append log event in log pipeline {}. Will be retried.", context.getName(), e);
        break;
      }

      metadata.put(iterator.getPartition(), new Checkpoint<>(eventQueue.getSmallestOffset(iterator.getPartition()),
                                                             event.getTimeStamp()));
      iterator.remove();
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

    // If no event was appended and the buffer is not full, so just return with 0 events appended.
    if (eventsAppended == 0) {
      // nothing has been appended so there is no metadata associated with those events. So return null.
      return new ProcessedEventMetadata<>(0, null);
    }

    metricsContext.gauge(Constants.Metrics.Name.Log.PROCESS_MIN_DELAY, minDelay);
    metricsContext.gauge(Constants.Metrics.Name.Log.PROCESS_MAX_DELAY, maxDelay);
    metricsContext.increment(Constants.Metrics.Name.Log.PROCESS_MESSAGES_COUNT, eventsAppended);

    return new ProcessedEventMetadata<>(eventsAppended, metadata);
  }

  /**
   * Returns if the queue is empty for a given partition.
   */
  public boolean isQueueEmpty(int partition) {
    return eventQueue.isEmpty(partition);
  }
}
