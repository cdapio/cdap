/*
 * Copyright 2014 Cask, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.internal.app.queue;

import co.cask.cdap.app.queue.InputDatum;
import co.cask.cdap.app.queue.QueueReader;
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
