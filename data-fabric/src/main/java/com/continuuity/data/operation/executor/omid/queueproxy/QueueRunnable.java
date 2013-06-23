package com.continuuity.data.operation.executor.omid.queueproxy;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.ttqueue.StatefulQueueConsumer;

/**
 * A QueueRunnable is used to define a queue operation that does not return a value.
 * QueueRunnable will be run by the QueueStateProxy.
 */
public abstract class QueueRunnable extends QueueOperation<Void> {
  /**
   * The run method will be called when the QueueRunnable is executed.
   * @param statefulQueueConsumer QueueConsumer with state that needs to be passed to all queue methods.
   * @throws com.continuuity.api.data.OperationException
   */
  public abstract void run(StatefulQueueConsumer statefulQueueConsumer) throws OperationException;

  /**
   * If the statefulQueueConsumer changes outside of the JVM during the queue operation then
   * this method is used let QueueStateProxy know about the change.
   * @param statefulQueueConsumer Updated statefulQueueConsumer
   */
  public void updateStatefulConsumer(StatefulQueueConsumer statefulQueueConsumer) {
    this.statefulQueueConsumer = statefulQueueConsumer;
  }

  @Override
  protected Void execute(StatefulQueueConsumer statefulQueueConsumer) throws OperationException {
    run(statefulQueueConsumer);
    return null;
  }
}
