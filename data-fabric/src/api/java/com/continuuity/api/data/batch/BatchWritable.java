/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.data.batch;

import com.continuuity.api.data.OperationException;

/**
 * Defines interface for the dataset that batch job can output into.
 * @param <KEY> the key type
 * @param <VALUE> the value type
 */
public interface BatchWritable<KEY, VALUE> {
  /**
   * Writes {key, value} record into dataset.
   * @param key key of the record
   * @param value value of the record
   * @throws OperationException if there's an error during write operation
   */
  void write(KEY key, VALUE value) throws OperationException;
}
