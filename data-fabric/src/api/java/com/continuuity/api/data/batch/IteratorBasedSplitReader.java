/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.data.batch;

import java.util.Iterator;

/**
 * Handy implementation of {@link SplitReader} backed by {@link Iterator}.
 * @param <KEY> the key type
 * @param <VALUE> the value type
 */
public abstract class IteratorBasedSplitReader<KEY, VALUE>
  extends SplitReaderBase<KEY, VALUE> {
  private Iterator<VALUE> iterator;

  /**
   * Creates iterator to iterate through all records of a given split.
   * @param split split to iterate through
   * @return an instance of {@link Iterator}
   */
  protected abstract Iterator<VALUE> createIterator(Split split);

  /**
   * Gets key from the given value provided by iterator.
   * @param value value to get key from
   * @return key
   */
  protected abstract KEY getKey(VALUE value);

  @Override
  public void initialize(final Split split) throws InterruptedException {

    iterator = createIterator(split);
  }

  @Override
  protected boolean fetchNextKeyValue() {
    if (!iterator.hasNext()) {
      return false;
    } else {
      VALUE next = iterator.next();
      KEY key = getKey(next);
      setCurrentKeyValue(key, next);
      return true;
    }
  }
}
