package com.continuuity.data.engine;

import com.continuuity.data.operation.queue.QueueConfig;
import com.continuuity.data.operation.queue.QueueConsumer;
import com.continuuity.data.operation.queue.QueueEntry;

public interface SimpleQueueTable {

  public boolean push(byte [] queueName, byte [] value);

  public QueueEntry pop(byte [] queueName, QueueConsumer consumer,
      QueueConfig config, boolean drain)
  throws InterruptedException;

  public boolean ack(byte [] queueName, QueueEntry entry);

}
