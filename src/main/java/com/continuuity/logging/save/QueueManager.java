package com.continuuity.logging.save;

import com.continuuity.logging.kafka.KafkaLogEvent;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Partitions KafkaLogEvents into different queues based on LoggingContext. This class is not thread-safe.
 */
public class QueueManager {
  private static final Logger LOG = LoggerFactory.getLogger(QueueManager.class);
  private final Map<String, BlockingQueue<KafkaLogEvent>> allQueues;
  private final BlockingQueue<BlockingQueue<KafkaLogEvent>> processingQueue;
  private final long eventProcessingDelayMs;

  /**
   * Creates a QueueManager.
   * @param eventProcessingDelayMs amount of time to wait before returning an event in drainLogEvents call.
   */
  public QueueManager(long eventProcessingDelayMs) {
    allQueues = Maps.newConcurrentMap();
    processingQueue = Queues.newPriorityBlockingQueue();

    this.eventProcessingDelayMs = eventProcessingDelayMs;
  }

  /**
   * Adds a KafkaLogEvent to the right queue based on LoggingContext. If the event does not contain a LoggingContext,
   * it will be dropped.
   * @param event KafkaLogEvent
   */
  public void addLogEvent(KafkaLogEvent event) {
    // Add event into right queue for processing
    String queueName = getQueueName(event);
    if (queueName == null) {
      LOG.debug(String.format("path fragment is null for event %s. Skipping it.", event));
      return;
    }
    BlockingQueue<KafkaLogEvent> queue = getQueue(queueName);
    queue.add(event);
  }

  /**
   * Drains KafkaLogEvents of a queue into the supplied list.
   * @param events list to drain the events into. The list will not be truncated by this method.
   * @param maxElements maximum number of elements to return.
   * @throws InterruptedException
   */
  public void drainLogEvents(List<KafkaLogEvent> events, int maxElements) throws InterruptedException {
    final long currentTs = System.currentTimeMillis();
    final int numQueues = processingQueue.size();
    int numTries = 0;
    while (numTries++ < numQueues) {
      BlockingQueue<KafkaLogEvent> queue = processingQueue.poll(200, TimeUnit.MILLISECONDS);

      KafkaLogEvent earliestEvent = (queue == null ? null : queue.peek());
      if (earliestEvent == null || earliestEvent.getLogEvent().getTimeStamp() + eventProcessingDelayMs >= currentTs) {
        if (queue != null) {
          processingQueue.add(queue);
        }
        continue;
      }

      // At least one event is available
      int numElements = 0;
      while (!queue.isEmpty() && numElements++ < maxElements) {
        if (queue.peek().getLogEvent().getTimeStamp() + eventProcessingDelayMs >= currentTs) {
          break;
        }
        events.add(queue.poll());
      }
      processingQueue.add(queue);
    }
  }

  private String getQueueName(KafkaLogEvent event) {
    return event.getLoggingContext().getLogPathFragment();
  }

  private BlockingQueue<KafkaLogEvent> getQueue(String name) {
    BlockingQueue<KafkaLogEvent> queue = allQueues.get(name);
    if (queue == null) {
      LOG.info(String.format("Creating queue with name %s", name));
      queue = new PriorityBlockingQueue<KafkaLogEvent>(1000, LOGGING_EVENT_COMPARATOR);
      allQueues.put(name, queue);
      processingQueue.add(queue);
    }
    return queue;
  }

  private static final Comparator<KafkaLogEvent> LOGGING_EVENT_COMPARATOR =
    new Comparator<KafkaLogEvent>() {
      @Override
      public int compare(KafkaLogEvent e1, KafkaLogEvent e2) {
        long e1Timestamp = e1.getLogEvent().getTimeStamp();
        long e2Timestamp = e2.getLogEvent().getTimeStamp();
        return e1Timestamp == e2Timestamp ? 0 : (e1Timestamp > e2Timestamp ? 1 : -1);
      }
    };
}
