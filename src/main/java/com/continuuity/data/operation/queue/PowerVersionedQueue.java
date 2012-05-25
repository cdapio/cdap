package com.continuuity.data.operation.queue;

import com.continuuity.data.engine.ReadPointer;

public interface PowerVersionedQueue {

  public boolean push(byte [] value, ReadPointer readPointer,
      long writeVersion);

  public QueueEntry pop(QueueConsumer consumer, QueueConfig config,
      boolean drain, ReadPointer readPointer, long writeVersion)
          throws InterruptedException;

  public boolean ack(QueueEntry entry, ReadPointer readPointer, long version);
}
