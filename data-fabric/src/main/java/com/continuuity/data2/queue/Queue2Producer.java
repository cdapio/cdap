/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.queue;

import java.io.IOException;

/**
 * TODO: This class should be renamed as QueueProducer when the old queue is gone.
 *
 * Implementation of this interface enqueues methods should be thread safe.
 */
public interface Queue2Producer {

  /**
   * Enqueues a {@link QueueEntry}.
   */
  void enqueue(QueueEntry entry) throws IOException;

  /**
   * Enqueues a list of {@link QueueEntry}.
   */
  void enqueue(Iterable<QueueEntry> entries) throws IOException;
}
