/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.queue;

import com.google.common.collect.ImmutableList;

import java.util.Collection;

/**
 *
 */
public interface DequeueResult {

  /**
   * Returns {@code true} if there is no data in the queue.
   */
  boolean isEmpty();


  /**
   * Returns entries being dequeued. If the dequeue result is empty, this method returns an empty collection.
   */
  Collection<byte[]> getData();

  /**
   * An (immutable) empty dequeue result
   */
  static final DequeueResult EMPTY_RESULT = new DequeueResult() {
    @Override
    public boolean isEmpty() {
      return true;
    }

    @Override
    public Collection<byte[]> getData() {
      return ImmutableList.of();
    }
  };
}
