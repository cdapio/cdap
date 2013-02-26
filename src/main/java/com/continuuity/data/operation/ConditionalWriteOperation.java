package com.continuuity.data.operation;

/**
 * An {@link Operation} that writes data, is atomic, but is not retryable.
 */
public abstract class ConditionalWriteOperation extends WriteOperation {
  protected ConditionalWriteOperation() {
  }

  protected ConditionalWriteOperation(long id) {
    super(id);
  }
}
