package com.continuuity.logging.save;

import com.continuuity.logging.kafka.KafkaLogEvent;

import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 *
 */
public class MessageQueue {
  private final PriorityBlockingQueue<KafkaLogEvent> queue;

  public MessageQueue() {
    this.queue = new PriorityBlockingQueue<KafkaLogEvent>(1000, LOGGING_EVENT_COMPARATOR);
  }

  public BlockingQueue<KafkaLogEvent> getQueue() {
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
