/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.data.batch;

/**
 * Interface for the dataset that a batch job can output to.
 * @param <KEY> The key type.
 * @param <VALUE> The value type.
 */
public interface BatchWritable<KEY, VALUE> {
  /**
   * Writes the {key, value} record into a dataset.
   *
   * @param key Key of the record.
   * @param value Value of the record.
   */
  void write(KEY key, VALUE value);
}
