package com.continuuity.app.queue;

import com.continuuity.api.data.OperationException;

/**
 * This interface defines reading of a {@link com.continuuity.internal.app.queue.QueueInputDatum} from the
 * Queue.
 */
public interface QueueReader {

  /**
   * Reads an input from the queue.
   *
   * @return A {@link com.continuuity.internal.app.queue.QueueInputDatum} which
   *         represents the input being read from the queue.
   * @throws OperationException If fails to dequeue.
   */
  InputDatum dequeue() throws OperationException;
}
