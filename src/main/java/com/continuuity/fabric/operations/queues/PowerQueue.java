/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.fabric.operations.queues;

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
  public QueueEntry pop(QueueConsumer consumer, QueuePartitioner partitioner)
      throws InterruptedException;

  /**
   * @param entry
   * @return
   */
  public boolean ack(QueueEntry entry);
}
