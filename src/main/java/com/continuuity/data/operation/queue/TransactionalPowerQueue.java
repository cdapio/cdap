/**
 * Copyright (C) 2012 Continuuity, Inc.
 */
package com.continuuity.data.operation.queue;

/**
 * Transactional Powerful Queues.
 * 
 * This is the interface for queues utilized by flows.
 */
public interface TransactionalPowerQueue {
  /**
   * @param value
   * @return
   */
  public QueueEntry put(byte[] value);

  public boolean putAck(QueueEntry entry);

  /**
   * @param consumer
   * @param config
   * @paramd drain
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
