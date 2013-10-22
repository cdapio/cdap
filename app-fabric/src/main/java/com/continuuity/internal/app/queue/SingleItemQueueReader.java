package com.continuuity.internal.app.queue;

import com.continuuity.data2.OperationException;
import com.continuuity.app.queue.InputDatum;
import com.continuuity.app.queue.QueueReader;

/**
 * An implementation of {@link QueueReader} that always returns the same {@link InputDatum}.
 * Each {@link #dequeue()} call would also increment the retry count of the given {@link InputDatum} by 1.
 */
public class SingleItemQueueReader implements QueueReader {

  private final InputDatum input;

  public SingleItemQueueReader(InputDatum input) {
    this.input = input;
  }

  @Override
  public InputDatum dequeue() throws OperationException {
    input.incrementRetry();
    return input;
  }
}
