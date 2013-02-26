package com.continuuity.data.operation;

/**
 * An {@link Operation} that writes data, is atomic, and is retryable.
 */
public abstract class WriteOperation extends Operation {

  public WriteOperation() {
    // nothing to do
  }

  public WriteOperation(long id) {
    super(id);
  }

  /**
   * Returns the key that this write operation acts upon.
   * @return the key this write operation acts upon
   */
  public abstract byte [] getKey();

  /**
   * Returns the priority of this write operation, where the priority determines
   * the order that this write operation will execute within a batched
   * transaction.  Lower priorities sort first.  User operations should all be
   * priority=1, enqueue=2, ack=3.
   * @return the priority of this write operation
   */
  public abstract int getPriority();

  /**
   * Returns the size of this write operation in terms of the amount of storage
   * that this write operation will utilize in the fabric.  This is used for
   * storage metrics of datasets.
   * @return size in bytes
   */
  public abstract int getSize();
}
