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
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * A {@link QueueReader} that reads from a list of {@link QueueReader}
 * in Round-Robin fashion. It will try skipping empty inputs when dequeueing
 * until a non-empty one is found or has exhausted the list of underlying
 * {@link QueueReader}, which will return an empty input.
 *
 * @param <T> Type of input dequeued from this reader.
 */

public final class RoundRobinQueueReader<T> extends TimeTrackingQueueReader<T> {

  private final InputDatum<T> nullInput = new NullInputDatum<T>();
  private final Iterator<QueueReader<T>> readers;

  public RoundRobinQueueReader(Iterable<QueueReader<T>> readers) {
    this.readers = Iterables.cycle(readers).iterator();
  }

  public InputDatum<T> tryDequeue(long timeout, TimeUnit timeoutUnit) throws IOException, InterruptedException {
    if (!readers.hasNext()) {
      return nullInput;
    }

    // Read an input from the underlying QueueReader
    QueueReader<T> begin = readers.next();
    InputDatum<T> input = begin.dequeue(timeout, timeoutUnit);

    // While the input is empty, keep trying to read from subsequent readers,
    // until a non-empty input is read or it loop back to the beginning reader.
    while (!input.needProcess()) {
      QueueReader<T> reader = readers.next();
      if (reader == begin) {
        return input;
      }
      input = reader.dequeue(0, TimeUnit.MILLISECONDS);
    }
    return input;
  }
}
