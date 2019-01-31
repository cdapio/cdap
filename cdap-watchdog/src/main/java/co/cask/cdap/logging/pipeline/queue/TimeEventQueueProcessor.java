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
import co.cask.cdap.logging.pipeline.LogPipelineConfig;
import co.cask.cdap.logging.pipeline.LogProcessorPipelineContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The {@link TimeEventQueue} processor to equeue the log events, and decides to append log events when max time
 * limit or size limit has reached.
 */
public class TimeEventQueueProcessor<Offset extends Comparable<Offset>> {
  private static final Logger LOG = LoggerFactory.getLogger(TimeEventQueueProcessor.class);
  private static final double MIN_FREE_FACTOR = 0.5d;
  private final String name;
  private final TimeEventQueue<ILoggingEvent, Offset> eventQueue;
  private final LogProcessorPipelineContext context;
  private final MetricsContext metricsContext;
  private final LogPipelineConfig config;

  public TimeEventQueueProcessor(String name, LogProcessorPipelineContext context, LogPipelineConfig config,
                                 Iterable<Integer> partitions) {
    this.name = name;
    this.eventQueue = new TimeEventQueue<>(partitions);
    this.context = context;
    this.metricsContext = context;
    this.config = config;
  }

  /**
   * Append and queue the event to the time queue.
   *
   * If the buffer is full, flush it first and then add the event.
   *      IF the buffer was not flushed, do not enqueue the event
   *      If the buffer was flushed, enqueue the event and return the metadata.
   *
   * If the buffer is not full
   *      If its time to append, append the events, enqueue the event  and return metadata
   *      If its not time to append, enqueue the event and return metadata.
   *
   * // TODO Also add a method which takes iterator of events
   */
  public AppendedEventMetadata<Offset> appendAndEnqueue(ILoggingEvent event, int eventSize,
                                                        int partition, Offset offset) {
    AppendedEventMetadata<Offset> metadata = append();
    // if the queue is not full, enqueue the log event
    if (metadata.getEventsAppended() < 0) {
      eventQueue.add(event, event.getTimeStamp(), eventSize, partition, offset);
    }
    return metadata;
  }

  /**
   * Decide whether to call all the appenders and append logs.
   *
   * @return Appended events metadata
   */
  private AppendedEventMetadata<Offset> append() {
    long minEventTime = System.currentTimeMillis() - config.getEventDelayMillis();
    long maxRetainSize = getMaxRetainSize();
    boolean forceAppend = maxRetainSize != Long.MAX_VALUE;

    int eventsAppended = 0;
    long minDelay = Long.MAX_VALUE;
    long maxDelay = -1;
    Map<Integer, CheckpointMetadata<Offset>> metadata = new HashMap<>();

    TimeEventQueue.EventIterator<ILoggingEvent, Offset> iterator = eventQueue.iterator();
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
        LOG.warn("Failed to append log event in pipeline {}. Will be retried.", name, e);
        break;
      }

      int partition = iterator.getPartition();
      metadata.put(iterator.getPartition(), new CheckpointMetadata<>(event.getTimeStamp(),
                                                                     eventQueue.getSmallestOffset(partition),
                                                                     eventQueue.isEmpty(partition)));
      iterator.remove();
      eventsAppended++;
    }

    // Always try to call flush, even there was no event written. This is needed so that appender get called
    // periodically even there is no new events being appended to perform housekeeping work.
    // Failure to flush is ok and it will be retried by the wrapped appender
    try {
      context.flush();
    } catch (IOException e) {
      LOG.warn("Failed to flush in pipeline {}. Will be retried.", name, e);
    }
    metricsContext.gauge("event.queue.size.bytes", eventQueue.getEventSize());

    // Even though the append was forced, for some reason none of the events got appended from the event queue. This
    // means caller should not append more events.
    if (forceAppend && eventsAppended == 0) {
      return new AppendedEventMetadata<>(-1, null);
    }

    // If no event was appended and the buffer is not full, so just return with 0 events appended.
    if (eventsAppended == 0) {
      // nothing has been appended so there is no metadata associated with those events. So return null.
      return new AppendedEventMetadata<>(0, null);
    }

    metricsContext.gauge(Constants.Metrics.Name.Log.PROCESS_MIN_DELAY, minDelay);
    metricsContext.gauge(Constants.Metrics.Name.Log.PROCESS_MAX_DELAY, maxDelay);
    metricsContext.increment(Constants.Metrics.Name.Log.PROCESS_MESSAGES_COUNT, eventsAppended);

    return new AppendedEventMetadata<>(eventsAppended, metadata);
  }

  /**
   * Calculates max retain size for event queue.
   *
   * @return
   */
  private long getMaxRetainSize() {
    long maxRetainSize = eventQueue.getEventSize() >= config.getMaxBufferSize()?
      (long) (config.getMaxBufferSize() * MIN_FREE_FACTOR) : Long.MAX_VALUE;

    if (maxRetainSize != Long.MAX_VALUE) {
      LOG.info("Maximum queue size {} reached for pipeline {}.", config.getMaxBufferSize(), name);
    }
    return maxRetainSize;
  }
}
