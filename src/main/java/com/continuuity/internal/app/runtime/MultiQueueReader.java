package com.continuuity.internal.app.runtime;

import com.continuuity.api.data.OperationException;

/**
 *
 */
public abstract class MultiQueueReader implements QueueReader {

  @Override
  public final InputDatum dequeue() throws OperationException {
    return selectQueueReader().dequeue();
  }

  protected abstract QueueReader selectQueueReader();
}
