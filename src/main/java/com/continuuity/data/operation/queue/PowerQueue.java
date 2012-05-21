/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.operation.queue;

/**
 * Powerful Queues.
 * 
 * This is the interface for queues utilized by flows.
 */
public interface PowerQueue {
  /**
   * @param value
   * @return
   */
  public boolean push(byte[] value);

  /**
   * @param consumer
   * @param partitioner
   * @return
   * @throws InterruptedException
   */
  public QueueEntry pop(QueueConsumer consumer, QueueConfig config,
      boolean drain)
  throws InterruptedException;

  /**
   * @param entry
   * @return
   */
  public boolean ack(QueueEntry entry);
}
