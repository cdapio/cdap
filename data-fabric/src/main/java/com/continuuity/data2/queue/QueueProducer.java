/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.queue;

import java.io.IOException;

/**
 * Implementation of this interface enqueues methods should be thread safe.
 */
public interface QueueProducer {

  /**
   * Enqueues a {@link QueueEntry}.
   */
  void enqueue(QueueEntry entry) throws IOException;

  /**
   * Enqueues a list of {@link QueueEntry}.
   */
  void enqueue(Iterable<QueueEntry> entries) throws IOException;
}
