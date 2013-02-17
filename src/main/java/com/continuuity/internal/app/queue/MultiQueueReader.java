package com.continuuity.internal.app.queue;

import com.continuuity.api.data.OperationException;
import com.continuuity.internal.app.runtime.InputDatum;

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
