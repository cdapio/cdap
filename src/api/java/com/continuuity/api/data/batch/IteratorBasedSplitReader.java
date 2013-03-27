/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.data.batch;

import com.continuuity.api.data.OperationException;

import java.util.Iterator;

/**
 * Handy implementation of {@link SplitReader} backed by {@link Iterator}
 */
public abstract class IteratorBasedSplitReader<KEY, VALUE>
  extends SplitReaderBase<KEY, VALUE> {
  private Iterator<VALUE> iterator;

  /**
   * Creates iterator to iterate through all records of a given split
   * @param dataset dataset that owns a split
   * @param split split to iterate through
   * @return an instance of {@link Iterator}
   * @throws OperationException if there's an error during reading the split
   */
  protected abstract Iterator<VALUE> createIterator(final BatchReadable dataset,
                                 final Split split) throws OperationException;

  /**
   * Gets key from the given value provided by iterator
   * @param value value to get key from
   * @return key
   */
  protected abstract KEY getKey(VALUE value);

  @Override
  public void initialize(final BatchReadable table,
                         final Split split) throws InterruptedException, OperationException {

    iterator = createIterator(table, split);
  }

  @Override
  protected boolean fetchNextKeyValue() throws OperationException {
    if (!iterator.hasNext()) {
      return false;
    } else {
      VALUE next = iterator.next();
      // TODO: is there a way to enforce VALUE to be "extends <WithKey<KEY>>"?
      KEY key = getKey(next);
      setCurrentKeyValue(key, next);
      return true;
    }
  }
}
