package com.continuuity.data2.transaction.queue;

import com.continuuity.common.queue.QueueName;
import com.google.common.collect.Maps;

import java.util.concurrent.ConcurrentMap;

/**
 * Maintains all in-memory queues in the system.
 */
public final class InMemoryQueueService {

  private static final ConcurrentMap<String, InMemoryQueue> queues = Maps.newConcurrentMap();

  public static InMemoryQueue getQueue(QueueName queueName) {
    String name = queueName.toString();
    InMemoryQueue queue = queues.get(name);
    if (queue == null) {
      queue = new InMemoryQueue();
      InMemoryQueue existing = queues.putIfAbsent(name, queue);
      if (existing != null) {
        queue = existing;
      }
    }
    return queue;
  }

  public static void dumpInfo() {
    for (String qname : queues.keySet()) {
      System.out.println("Queue '" + qname + "': size is " + queues.get(qname).getSize());
    }
  }

}
