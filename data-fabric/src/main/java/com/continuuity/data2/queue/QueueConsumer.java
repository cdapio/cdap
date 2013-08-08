/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.queue;

import java.io.IOException;

/**
 *
 */
public interface QueueConsumer {

  /**
   * Dequeue an entry from the queue.
   * @return A {@link DequeueResult}.
   */
  DequeueResult dequeue() throws IOException;

  /**
   * Dequeue multiple entries from the queue. The dequeue result may have less entries than the given
   * maxBatchSize, depending on how many entries in the queue.
   * @param maxBatchSize Maximum number of entries to queue.
   * @return A {@link DequeueResult}.
   */
  DequeueResult dequeue(int maxBatchSize) throws IOException;
}
