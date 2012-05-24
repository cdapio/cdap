package com.continuuity.data.engine;

import com.continuuity.data.operation.queue.QueueConsumer;
import com.continuuity.data.operation.queue.QueueEntry;
import com.continuuity.data.operation.queue.QueuePartitioner;

public interface VersionedQueueTable extends SimpleQueueTable {

  public boolean push(byte [] queueName, byte [] value, ReadPointer readPointer,
      long writeVersion);

  public QueueEntry pop(byte [] queueName, QueueConsumer consumer,
      QueuePartitioner partitioner, ReadPointer readPointer, long writeVersion)
  throws InterruptedException;

  public boolean ack(byte [] queueName, QueueEntry entry,
      ReadPointer readPointer, long writeVersion);

}
