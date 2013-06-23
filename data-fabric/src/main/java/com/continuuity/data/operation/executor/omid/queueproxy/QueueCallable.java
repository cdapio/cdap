package com.continuuity.data.operation.executor.omid.queueproxy;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.ttqueue.StatefulQueueConsumer;

/**
 * A QueueCallable is used to define a queue operation that returns a value.
 * QueueCallable will be run by the QueueStateProxy.
 * @param <T> Type of the return value
 */
public abstract class QueueCallable<T> extends QueueOperation<T> {
  /**
   * The call method will be called when the QueueCallable is executed.
   * @param statefulQueueConsumer QueueConsumer with state that needs to be passed to all queue methods.
   * @return Return value of the queue operation
   * @throws com.continuuity.api.data.OperationException
   */
  public abstract T call(StatefulQueueConsumer statefulQueueConsumer) throws OperationException;

  /**
   * If the statefulQueueConsumer changes outside of the JVM during the queue operation then
   * this method is used let QueueStateProxy know about the change.
   * @param statefulQueueConsumer Updated statefulQueueConsumer
   */
  public void updateStatefulConsumer(StatefulQueueConsumer statefulQueueConsumer) {
    this.statefulQueueConsumer = statefulQueueConsumer;
  }

  @Override
  protected T execute(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
    return call(statefulQueueConsumer);
  }
}
