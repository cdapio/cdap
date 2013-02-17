package com.continuuity.internal.app.runtime;

import com.continuuity.api.data.OperationException;

/**
 *
 */
public interface QueueReader {

  /**
   * Reads an input from the queue.
   * @return A {@link InputDatum} which represents the input being read from the queue.
   * @throws OperationException If fails to dequeue.
   */
  InputDatum dequeue() throws OperationException;
}
