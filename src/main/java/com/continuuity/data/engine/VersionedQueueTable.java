package com.continuuity.data.engine;

import com.continuuity.data.operation.queue.QueueConfig;
import com.continuuity.data.operation.queue.QueueConsumer;
import com.continuuity.data.operation.queue.QueueEntry;

public interface VersionedQueueTable {

  public boolean push(byte [] queueName, byte [] value,
      ReadPointer readPointer, long writeVersion);

  public QueueEntry pop(byte [] queueName, QueueConsumer consumer,
      QueueConfig config, boolean drain, ReadPointer readPointer,
      long writeVersion)
  throws InterruptedException;

  public boolean ack(byte [] queueName, QueueEntry entry,
      ReadPointer readPointer, long writeVersion);

}
