package com.continuuity.data.operation.executor.omid.queueproxy;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.ttqueue.StatefulQueueConsumer;

/**
*
*/
public abstract class QueueOperation<T> {
  protected volatile StatefulQueueConsumer statefulQueueConsumer = null;

  protected abstract T execute(StatefulQueueConsumer statefulQueueConsumer) throws OperationException;
  protected abstract void updateStatefulConsumer(StatefulQueueConsumer statefulQueueConsumer);
}
