package com.continuuity.data2.transaction.queue.inmemory;

import com.continuuity.common.queue.QueueName;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
   * Drop either all streams or all queues.
   * @param clearStreams if true, drops all streams, if false, clears all queues.
   * @param prefix if non-null, drops only queues with a name that begins with this prefix.
   */
  private void resetAllQueuesOrStreams(boolean clearStreams, @Nullable String prefix) {
    List<String> toRemove = Lists.newArrayListWithCapacity(queues.size());
    for (String queueName : queues.keySet()) {
      if ((clearStreams && QueueName.isStream(queueName)) || (!clearStreams && QueueName.isQueue(queueName))) {
        if (prefix == null ||  queueName.startsWith(prefix)) {
          toRemove.add(queueName);
        }
      }
    }
    for (String queueName : toRemove) {
      queues.remove(queueName);
    }
  }

  public void resetQueues() {
    resetAllQueuesOrStreams(false, null);
  }

  public void resetQueuesWithPrefix(String prefix) {
    resetAllQueuesOrStreams(false, prefix);
  }

  public void resetStreams() {
    resetAllQueuesOrStreams(true, null);
  }

  public boolean exists(String queueName) {
    return queues.containsKey(queueName);
  }

  public void truncate(String queueName) {
    InMemoryQueue queue = queues.get(queueName);
    if (queue != null) {
      queue.clear();
    }
  }

  /**
   * Clear all streams or queues with a given prefix.
   * @param prefix the prefix to match.
   */
  public void truncateAllWithPrefix(@Nonnull String prefix) {
    for (String queueName : queues.keySet()) {
      if (queueName.startsWith(prefix)) {
        truncate(queueName);
      }
    }
  }

  public void drop(String queueName) {
    queues.remove(queueName);
  }
}
