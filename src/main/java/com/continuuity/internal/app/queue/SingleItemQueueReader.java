package com.continuuity.internal.app.queue;

import com.continuuity.api.data.OperationException;
import com.continuuity.app.queue.QueueReader;
import com.continuuity.internal.app.runtime.InputDatum;

/**
 * An implementation of {@link QueueReader} that always returns the same {@link com.continuuity.internal.app.runtime.QueueInputDatum}.
 * Each {@link #dequeue()} call would also increment the retry count of the given {@link com.continuuity.internal.app.runtime.QueueInputDatum} by 1.
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
