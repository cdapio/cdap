package com.continuuity.app.queue;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * This interface defines reading of a {@link InputDatum} from the Queue.
 *
 * @param <T> Type of input dequeued from this reader.
 */
public interface QueueReader<T> {

  /**
   * Reads an input from the queue.
   *
   * @param timeout Maximum time for trying to have a non-empty dequeue result.
   *                Depending on the implementation, actual time spent for dequeue
   *                could be longer than the time specified here.
   * @param timeoutUnit Unit for the timeout.
   *
   * @return A {@link InputDatum} which
   *         represents the input being read from the queue.
   * @throws IOException If fails to dequeue.
   * @throws InterruptedException If dequeue is interrupted.
   */
  InputDatum<T> dequeue(long timeout, TimeUnit timeoutUnit) throws IOException, InterruptedException;
}
