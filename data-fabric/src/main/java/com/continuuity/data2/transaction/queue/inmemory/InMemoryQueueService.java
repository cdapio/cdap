package com.continuuity.data2.transaction.queue.inmemory;

import com.continuuity.common.queue.QueueName;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.io.PrintStream;
import java.util.List;
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

  /**
   * Clear either all streams or all queues.
   * @param clearStreams if true, clears all streams, if false, clears all queues.
   */
  private void resetAllQueuesOrStreams(boolean clearStreams) {
    List<String> toRemove = Lists.newArrayListWithCapacity(queues.size());
    for (String queueName : queues.keySet()) {
      if ((clearStreams && QueueName.isStream(queueName)) || (!clearStreams && QueueName.isQueue(queueName))) {
        toRemove.add(queueName);
      }
    }
    for (String queueName : toRemove) {
      queues.remove(queueName);
    }
  }

  public void resetQueues() {
    resetAllQueuesOrStreams(false);
  }

  public void resetStreams() {
    resetAllQueuesOrStreams(true);
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
