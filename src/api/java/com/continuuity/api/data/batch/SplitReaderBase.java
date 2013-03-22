package com.continuuity.api.data.batch;

import com.continuuity.api.data.OperationException;

public abstract class SplitReaderBase<KEY, VALUE> extends SplitReader<KEY, VALUE> {
  private KEY currentKey;
  private VALUE currentValue;

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
