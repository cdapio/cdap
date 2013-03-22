package com.continuuity.api.data.batch;


import com.continuuity.api.data.OperationException;

import java.util.Iterator;

public abstract class IteratorBasedSplitReader<KEY, VALUE extends WithKey> extends SplitReaderBase<KEY, VALUE> {
  private Iterator<VALUE> iterator;

  protected abstract Iterator<VALUE> createIterator(final BatchReadable table,
                                 final Split split) throws OperationException;

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
      @SuppressWarnings("unchecked")
      KEY key = (KEY) next.getKey();
      setCurrentKeyValue(key, next);
      return true;
    }
  }

}
