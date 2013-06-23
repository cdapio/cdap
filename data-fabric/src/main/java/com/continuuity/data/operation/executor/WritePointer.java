package com.continuuity.data.operation.executor;

/**
 * Interface defines the write version of a transaction.
 */
public interface WritePointer {
  /**
   * Returns the write version.
   * @return write version
   */
  long getWriteVersion();
}
