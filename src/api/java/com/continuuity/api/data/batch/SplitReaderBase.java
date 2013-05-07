/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.data.batch;

import com.continuuity.api.data.OperationException;

/**
 * Provides handy abstract implementation of {@link SplitReader}.
 * <p>
 *   Iterates over Split data using {@link #fetchNextKeyValue()} method.
 * </p>
 * @param <KEY> the key type
 * @param <VALUE> the value type
 */
public abstract class SplitReaderBase<KEY, VALUE> extends SplitReader<KEY, VALUE> {
  private KEY currentKey;
  private VALUE currentValue;

  /**
   * Fetches next data item of the split being read. Should use {@link #setCurrentKeyValue(Object, Object)} method to
   * provide next item read.
   * @return false if reached end of the split, true otherwise.
   * @throws OperationException
   */
  protected abstract boolean fetchNextKeyValue() throws OperationException;

  protected void setCurrentKeyValue(KEY key, VALUE value) {
    currentKey = key;
    currentValue = value;
  }

  @Override
  public boolean nextKeyValue() throws InterruptedException, OperationException {
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
