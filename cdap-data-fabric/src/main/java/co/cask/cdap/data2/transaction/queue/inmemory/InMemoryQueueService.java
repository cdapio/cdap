/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.transaction.queue.inmemory;

import co.cask.cdap.common.queue.QueueName;
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

  private final ConcurrentMap<QueueName, InMemoryQueue> queues;

  /**
   * Package visible constructor so that instance of this class can only be created through Guice.
   */
  @Inject
  private InMemoryQueueService() {
    queues = Maps.newConcurrentMap();
  }

  InMemoryQueue getQueue(QueueName queueName) {
    InMemoryQueue queue = queues.get(queueName);
    if (queue == null) {
      queue = new InMemoryQueue();
      InMemoryQueue existing = queues.putIfAbsent(queueName, queue);
      if (existing != null) {
        queue = existing;
      }
    }
    return queue;
  }

  @SuppressWarnings("unused")
  public void dumpInfo(PrintStream out) {
    for (QueueName qname : queues.keySet()) {
      out.println("Queue '" + qname + "': size is " + queues.get(qname).getSize());
    }
  }

  /**
   * Drop either all streams or all queues.
   * @param clearStreams if true, drops all streams, if false, clears all queues.
   * @param prefix if non-null, drops only queues with a name that begins with this prefix.
   */
  private void resetAllQueuesOrStreams(boolean clearStreams, @Nullable String prefix) {
    List<QueueName> toRemove = Lists.newArrayListWithCapacity(queues.size());
    for (QueueName queueName : queues.keySet()) {
      if ((clearStreams && queueName.isStream()) || (!clearStreams && queueName.isQueue())) {
        if (prefix == null ||  queueName.toString().startsWith(prefix)) {
          toRemove.add(queueName);
        }
      }
    }
    for (QueueName queueName : toRemove) {
      queues.remove(queueName);
    }
  }

  @SuppressWarnings("unused")
  public void resetQueues() {
    resetAllQueuesOrStreams(false, null);
  }

  public void resetQueuesWithPrefix(String prefix) {
    resetAllQueuesOrStreams(false, prefix);
  }

  @SuppressWarnings("unused")
  public void resetStreams() {
    resetAllQueuesOrStreams(true, null);
  }

  public void resetStreamsWithPrefix(String prefix) {
    resetAllQueuesOrStreams(true, prefix);
  }

  public boolean exists(QueueName queueName) {
    return queues.containsKey(queueName);
  }

  public void truncate(QueueName queueName) {
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
    for (QueueName queueName : queues.keySet()) {
      if (queueName.toString().startsWith(prefix)) {
        truncate(queueName);
      }
    }
  }

  public void drop(QueueName queueName) {
    queues.remove(queueName);
  }
}
