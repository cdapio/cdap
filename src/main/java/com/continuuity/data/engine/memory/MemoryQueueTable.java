package com.continuuity.data.engine.memory;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;

import com.continuuity.data.engine.SimpleQueueTable;
import com.continuuity.data.operation.queue.QueueConfig;
import com.continuuity.data.operation.queue.QueueConsumer;
import com.continuuity.data.operation.queue.QueueEntry;

public class MemoryQueueTable implements SimpleQueueTable {

  final Map<byte[],MemoryQueue> queues =
      new TreeMap<byte[],MemoryQueue>(Bytes.BYTES_COMPARATOR);

  private MemoryQueue getQueue(byte[] queueName) {
    synchronized (queues) {
      MemoryQueue queue = queues.get(queueName);
      if (queue == null) {
        queue = new MemoryQueue();
        queues.put(queueName, queue);
      }
      return queue;
    }
  }
  
  @Override
  public boolean push(byte[] queueName, byte[] value) {
    return getQueue(queueName).push(value);
  }

  @Override
  public QueueEntry pop(byte[] queueName, QueueConsumer consumer,
      QueueConfig config, boolean drain) throws InterruptedException {
    return getQueue(queueName).pop(consumer, config, drain);
  }

  @Override
  public boolean ack(byte[] queueName, QueueEntry entry) {
    return getQueue(queueName).ack(entry);
  }

}
