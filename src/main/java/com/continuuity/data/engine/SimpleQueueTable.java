package com.continuuity.data.engine;

import com.continuuity.data.operation.queue.QueueConsumer;
import com.continuuity.data.operation.queue.QueueEntry;
import com.continuuity.data.operation.queue.QueuePartitioner;

public interface SimpleQueueTable {

  public boolean push(byte [] queueName, byte [] value);

  public QueueEntry pop(byte [] queueName, QueueConsumer consumer,
      QueuePartitioner partitioner)
  throws InterruptedException;

  public boolean ack(byte [] queueName, QueueEntry entry);

}
