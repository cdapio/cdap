/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.data.batch;

/**
 * Defines a reader of a dataset {@link Split}.
 * @param <KEY> The key type.
 * @param <VALUE> The value type.
 */
public abstract class SplitReader<KEY, VALUE> {

  /**
   * Called once at initialization.
   * @param split The split that defines the range of records to read.
   * @throws InterruptedException
   */
  public abstract void initialize(Split split) throws InterruptedException;

  /**
   * Read the next key, value pair.
   * @return true if a key/value pair was read.
   * @throws InterruptedException
   */
  public abstract boolean nextKeyValue() throws InterruptedException;

  /**
   * Get the current key.
   * @return The current key, or null if there is no current key.
   * @throws InterruptedException
   */
  public abstract KEY getCurrentKey() throws InterruptedException;

  /**
   * Get the current value.
   * @return The current value of the object that was read.
   * @throws InterruptedException
   */
  public abstract VALUE getCurrentValue() throws InterruptedException;

  /**
   * The current progress of the record reader through its data.
   * By default progress is not reported in the middle of split reading.
   * @return A number between 0.0 and 1.0 that is the fraction of the data that has been read.
   * @throws InterruptedException
   */
  public float getProgress() throws InterruptedException {
    return 0;
  }

  /**
   * Close the record reader.
   */
  public abstract void close();
}
