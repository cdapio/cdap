package com.continuuity.data.operation.type;

/**
 * An {@link Operation} that writes data, is atomic, and is retryable.
 */
public interface WriteOperation extends Operation {

  public byte [] getKey();

  /**
   * Returns the priority of this write operation, where the priority determines
   * the order that this write operation will execute within a batched
   * transaction.  Lower priorities sort first.  User operations should all be
   * priority=1, enqueue=2, ack=3.
   * @return
   */
  public int getPriority();
}
