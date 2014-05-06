package com.continuuity.app.queue;

import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.common.queue.QueueName;

/**
 * Represents a dequeue result from {@link QueueReader}.
 *
 * @param <T> Type of input.
 */
public interface InputDatum<T> extends Iterable<T> {

  boolean needProcess();

  void incrementRetry();

  int getRetry();

  InputContext getInputContext();

  QueueName getQueueName();

  /**
   * Reclaim the input from the queue consumer. It is needed for processing retried entries.
   */
  void reclaim();

  /**
   * Returns number of entries in this Iterable.
   */
  int size();
}
