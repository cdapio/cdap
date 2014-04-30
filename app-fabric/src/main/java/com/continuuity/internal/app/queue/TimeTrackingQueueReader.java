/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.queue;

import com.continuuity.app.queue.InputDatum;
import com.continuuity.app.queue.QueueReader;
import com.google.common.base.Stopwatch;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * A {@link QueueReader} that track time spent when trying to dequeue.
 *
 * @param <T> Type of input dequeued from this reader.
 */
public abstract class TimeTrackingQueueReader<T> implements QueueReader<T> {

  // Maximum number of times to try to deuque an non-empty result.
  private static final int DEQUEUE_TRAILS = 10;

  @Override
  public final InputDatum<T> dequeue(long timeout,
                                     TimeUnit timeoutUnit) throws IOException, InterruptedException {
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    long sleepNano = computeSleepNano(timeout, timeoutUnit);
    long timeoutNano = timeoutUnit.toNanos(timeout);

    InputDatum<T> result = tryDequeue(timeout, timeoutUnit);
    while (!result.needProcess()) {
      if (timeoutNano <= 0) {
        break;
      }

      long elapsedNano = stopwatch.elapsedTime(TimeUnit.NANOSECONDS);
      if (elapsedNano + sleepNano >= timeoutNano) {
        break;
      }
      TimeUnit.NANOSECONDS.sleep(sleepNano);
      result = tryDequeue(timeoutNano - elapsedNano, TimeUnit.NANOSECONDS);
    }
    return result;
  }

  /**
   * Children class override it to try to dequeue.
   */
  protected abstract InputDatum<T> tryDequeue(long timeout, TimeUnit timeoutUnit)
                                              throws IOException, InterruptedException;

  private long computeSleepNano(long timeout, TimeUnit unit) {
    long sleepNano = TimeUnit.NANOSECONDS.convert(timeout, unit) / DEQUEUE_TRAILS;
    return sleepNano <= 0 ? 1 : sleepNano;
  }
}
