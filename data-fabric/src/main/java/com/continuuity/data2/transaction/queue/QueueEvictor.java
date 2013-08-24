/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.data2.transaction.Transaction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * For performing queue eviction action.
 */
public interface QueueEvictor {

  /**
   * Performs queue eviction in the given transaction context.
   * @param transaction The transaction context that the eviction happens.
   * @return A {@link ListenableFuture} that will be completed when eviction is done. The future result
   *         carries number of entries being evicted.
   */
  ListenableFuture<Integer> evict(Transaction transaction);

  static final QueueEvictor NOOP = new QueueEvictor() {

    @Override
    public ListenableFuture<Integer> evict(Transaction transaction) {
      return Futures.immediateFuture(0);
    }
  };
}
