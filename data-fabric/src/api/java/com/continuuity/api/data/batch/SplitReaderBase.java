/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.data.batch;

/**
 * Provides an abstract implementation of {@link SplitReader}.
 * <p>
 *   Iterates over split data using the {@link #fetchNextKeyValue()} method.
 * </p>
 * @param <KEY> The key type.
 * @param <VALUE> The value type.
 */
public abstract class SplitReaderBase<KEY, VALUE> extends SplitReader<KEY, VALUE> {
  private KEY currentKey;
  private VALUE currentValue;

  /**
   * Fetches the next data item of the split being read. 
   *
   * If true, use the {@link #setCurrentKeyValue(Object, Object)} method to
   * set the new current key/value. If false there are no more key/value records to read. 
   *
   * See {@link com.continuuity.api.data.batch.IteratorBasedSplitReader} for an implementation 
   * of the abstract fetchNextKeyValue() method.
   *
   * @return false if reached end of the split, true otherwise.
   */
  protected abstract boolean fetchNextKeyValue();

  protected void setCurrentKeyValue(KEY key, VALUE value) {
    currentKey = key;
    currentValue = value;
  }

  @Override
  public boolean nextKeyValue() throws InterruptedException {
    return fetchNextKeyValue();
  }

  @Override
  public KEY getCurrentKey() throws InterruptedException {
    return currentKey;
  }

  @Override
  public VALUE getCurrentValue() throws InterruptedException {
    return currentValue;
  }

  @Override
  public void close() {
  }
}
