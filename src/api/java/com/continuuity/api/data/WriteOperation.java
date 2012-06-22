package com.continuuity.api.data;

/**
 * An {@link Operation} that writes data, is atomic, and is retryable.
 */
public interface WriteOperation extends Operation {

  /**
   * Returns the key that this write operation acts upon.
   * @return the key this write operation acts upon
   */
  public byte [] getKey();

  /**
   * Returns the priority of this write operation, where the priority determines
   * the order that this write operation will execute within a batched
   * transaction.  Lower priorities sort first.  User operations should all be
   * priority=1, enqueue=2, ack=3.
   * @return the priority of this write operation
   */
  public int getPriority();
}
