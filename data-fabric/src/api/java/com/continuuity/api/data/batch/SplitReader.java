/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.data.batch;

import com.continuuity.api.data.OperationException;

/**
 * Defines a reader of dataset {@link Split}.
 * @param <KEY> the key type
 * @param <VALUE> the value type
 */
public abstract class SplitReader<KEY, VALUE> {

  /**
   * Called once at initialization.
   * @param split the split that defines the range of records to read
   * @throws InterruptedException
   */
  public abstract void initialize(Split split)
    throws InterruptedException, OperationException;

  /**
   * Read the next key, value pair.
   * @return true if a key/value pair was read
   * @throws InterruptedException
   */
  public abstract boolean nextKeyValue() throws InterruptedException, OperationException;

  /**
   * Get the current key.
   * @return the current key or null if there is no current key
   * @throws InterruptedException
   */
  public abstract KEY getCurrentKey() throws InterruptedException;

  /**
   * Get the current value.
   * @return the object that was read
   * @throws InterruptedException
   */
  public abstract VALUE getCurrentValue() throws InterruptedException, OperationException;

  /**
   * The current progress of the record reader through its data.
   * @return a number between 0.0 and 1.0 that is the fraction of the data read
   * @throws InterruptedException
   */
  public float getProgress() throws InterruptedException {
    // by default do not report progress in the middle of split reading
    return 0;
  }

  /**
   * Close the record reader.
   */
  public abstract void close();
}
