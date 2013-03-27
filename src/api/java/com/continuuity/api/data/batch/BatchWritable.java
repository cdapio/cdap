package com.continuuity.api.data.batch;

import com.continuuity.api.data.OperationException;

/**
 * Defines interface for the dataset that batch job can output into.
 */
public interface BatchWritable<KEY, VALUE> {
  /**
   * Writes {key, value} record into dataset
   * @param key key of the record
   * @param value value of the record
   * @throws OperationException if there's an error during write operation
   */
  void write(KEY key, VALUE value) throws OperationException;
}
