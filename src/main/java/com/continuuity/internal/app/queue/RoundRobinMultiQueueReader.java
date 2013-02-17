package com.continuuity.internal.app.queue;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import java.util.Iterator;

/**
 *
 */
public final class RoundRobinMultiQueueReader extends MultiQueueReader {

  private final Iterator<QueueReader> readers;

  public RoundRobinMultiQueueReader(QueueReader...readers) {
    Preconditions.checkArgument(readers.length > 0, "No QueueReader given.");
    this.readers = Iterables.cycle(readers).iterator();
  }

  public RoundRobinMultiQueueReader(Iterable<QueueReader> readers) {
    this.readers = Iterables.cycle(readers).iterator();
    Preconditions.checkArgument(this.readers.hasNext(), "No QueueReader given.");
  }

  @Override
  protected QueueReader selectQueueReader() {
    return readers.next();
  }
}
