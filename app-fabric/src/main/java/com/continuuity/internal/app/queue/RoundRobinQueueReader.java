package com.continuuity.internal.app.queue;

import com.continuuity.data2.OperationException;
import com.continuuity.app.queue.InputDatum;
import com.continuuity.app.queue.QueueReader;
import com.google.common.collect.Iterables;

import java.util.Iterator;

/**
 * A {@link QueueReader} that reads from a list of {@link QueueReader}
 * in Round-Robin fashion. It will try skipping empty inputs when dequeueing
 * until a non-empty one is found or has exhausted the list of underlying
 * {@link QueueReader}, which will return an empty input.
 */
public final class RoundRobinQueueReader implements QueueReader {

  private static final InputDatum NULL_INPUT = new NullInputDatum();

  private final Iterator<QueueReader> readers;

  public RoundRobinQueueReader(Iterable<QueueReader> readers) {
    this.readers = Iterables.cycle(readers).iterator();
  }

  @Override
  public InputDatum dequeue() throws OperationException {
    if (!readers.hasNext()) {
      return NULL_INPUT;
    }

    // Read an input from the underlying QueueReader
    QueueReader begin = readers.next();
    InputDatum input = begin.dequeue();

    // While the input is empty, keep trying to read from subsequent readers,
    // until a non-empty input is read or it loop back to the beginning reader.
    while (!input.needProcess()) {
      QueueReader reader = readers.next();
      if (reader == begin) {
        return input;
      }
      input = reader.dequeue();
    }
    return input;
  }
}
