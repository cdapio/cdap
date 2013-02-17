package com.continuuity.internal.app.queue;

import com.continuuity.api.data.OperationException;
import com.continuuity.internal.app.runtime.InputDatum;

/**
 *
 */
public interface QueueReader {

  /**
   * Reads an input from the queue.
   * @return A {@link com.continuuity.internal.app.runtime.InputDatum} which represents the input being read from the queue.
   * @throws OperationException If fails to dequeue.
   */
  InputDatum dequeue() throws OperationException;
}
