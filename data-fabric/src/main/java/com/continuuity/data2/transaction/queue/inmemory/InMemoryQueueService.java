package com.continuuity.data2.transaction.queue.inmemory;

import com.continuuity.common.queue.QueueName;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.io.PrintStream;
import java.util.concurrent.ConcurrentMap;

/**
 * Maintains all in-memory queues in the system.
 */
@Singleton
public final class InMemoryQueueService {

  private final ConcurrentMap<String, InMemoryQueue> queues;

  /**
   * Package visible constructor so that instance of this class can only be created through Guice.
   */
  @Inject
  private InMemoryQueueService() {
    queues = Maps.newConcurrentMap();
  }

  InMemoryQueue getQueue(QueueName queueName) {
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

  public void dumpInfo(PrintStream out) {
    for (String qname : queues.keySet()) {
      out.println("Queue '" + qname + "': size is " + queues.get(qname).getSize());
    }
  }

  public void reset() {
    queues.clear();
  }

  public boolean exists(String queueName) {
    return queues.containsKey(queueName);
  }

  public void truncate(String queueName) {
    queues.put(queueName, new InMemoryQueue());
  }

  public void drop(String queueName) {
    queues.remove(queueName);
  }
}
