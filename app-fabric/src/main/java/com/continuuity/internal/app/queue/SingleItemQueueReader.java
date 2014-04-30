package com.continuuity.internal.app.queue;

import com.continuuity.app.queue.InputDatum;
import com.continuuity.app.queue.QueueReader;

import java.util.concurrent.TimeUnit;

/**
 * An implementation of {@link QueueReader} that always returns the same {@link InputDatum}.
 * Each {@link #dequeue(long, java.util.concurrent.TimeUnit)} call would also increment
 * the retry count of the given {@link InputDatum} by 1.
 *
 * @param <T> Type of input dequeued from this reader.
 */
public class SingleItemQueueReader<T> implements QueueReader<T> {

  private final InputDatum<T> input;

  public SingleItemQueueReader(InputDatum<T> input) {
    this.input = input;
  }

  @Override
  public InputDatum<T> dequeue(long timeout, TimeUnit timeoutUnit) {
    input.incrementRetry();
    return input;
  }
}
