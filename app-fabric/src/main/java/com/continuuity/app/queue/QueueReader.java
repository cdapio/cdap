package com.continuuity.app.queue;

import com.continuuity.data2.OperationException;

/**
 * This interface defines reading of a {@link InputDatum} from the
 * Queue.
 */
public interface QueueReader {

  /**
   * Reads an input from the queue.
   *
   * @return A {@link InputDatum} which
   *         represents the input being read from the queue.
   * @throws OperationException If fails to dequeue.
   */
  InputDatum dequeue() throws OperationException;
}
