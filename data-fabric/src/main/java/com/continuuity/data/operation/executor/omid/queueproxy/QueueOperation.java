package com.continuuity.data.operation.executor.omid.queueproxy;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.ttqueue.StatefulQueueConsumer;

/**
* Represents a queue operation that can be run in {@link QueueStateProxy}.
* @param <T> operation parameter.
*/
public abstract class QueueOperation<T> {
  /**
   * When {@link #updateStatefulConsumer(com.continuuity.data.operation.ttqueue.StatefulQueueConsumer)} is used
   * by the operation to update the statefulQueueConsumer, the derived classes need to set the this field to pass the
   * update to {@link QueueStateProxy}.
   */
  protected volatile StatefulQueueConsumer statefulQueueConsumer = null;

  /**
   * This method is called by the {@link QueueStateProxy} to execute the operation.
   * Derived classes need to override this to define the operation.
   * @param statefulQueueConsumer QueueConsumer with state that needs to be passed to all queue methods.
   * @return the return value of the operation
   * @throws OperationException
   */
  protected abstract T execute(StatefulQueueConsumer statefulQueueConsumer) throws OperationException;

  /**
   * If the statefulQueueConsumer changes outside of the JVM during the queue operation then
   * this method is used let {@link QueueStateProxy} know about the change.
   * @param statefulQueueConsumer Updated statefulQueueConsumer
   */
  protected abstract void updateStatefulConsumer(StatefulQueueConsumer statefulQueueConsumer);
}
