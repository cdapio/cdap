package com.continuuity.logging.save;

import com.continuuity.logging.kafka.KafkaLogEvent;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Partitions KafkaLogEvents into different queues based on LoggingContext. This class is not thread-safe.
 */
public class QueueManager {
  private static final Logger LOG = LoggerFactory.getLogger(QueueManager.class);
  private final Map<String, MessageQueue> allQueues;
  private final BlockingQueue<MessageQueue> processingQueue;
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
    MessageQueue queue = getQueue(queueName);
    queue.getQueue().add(event);
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
      MessageQueue queue = processingQueue.poll(200, TimeUnit.MILLISECONDS);

      KafkaLogEvent earliestEvent = (queue == null ? null : queue.getQueue().peek());
      if (earliestEvent == null || earliestEvent.getLogEvent().getTimeStamp() + eventProcessingDelayMs >= currentTs) {
        if (queue != null) {
          processingQueue.add(queue);
        }
        continue;
      }

      // At least one event is available
      int numElements = 0;
      while (!queue.getQueue().isEmpty() && numElements++ < maxElements) {
        if (queue.getQueue().peek().getLogEvent().getTimeStamp() + eventProcessingDelayMs >= currentTs) {
          break;
        }
        events.add(queue.getQueue().poll());
      }
      processingQueue.add(queue);
    }
  }

  private String getQueueName(KafkaLogEvent event) {
    return event.getLoggingContext().getLogPathFragment();
  }

  private MessageQueue getQueue(String name) {
    MessageQueue messageQueue = allQueues.get(name);
    if (messageQueue == null) {
      LOG.info(String.format("Creating queue with name %s", name));
      messageQueue = new MessageQueue();
      allQueues.put(name, messageQueue);
      processingQueue.add(messageQueue);
    }
    return messageQueue;
  }
}
