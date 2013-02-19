package com.continuuity.internal.app.queue;

import com.continuuity.api.data.OperationException;
import com.continuuity.app.queue.QueueName;
import com.continuuity.app.queue.QueueReader;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.internal.app.runtime.InputDatum;

/**
 *
 */
public final class SingleQueueReader implements QueueReader {

  private final OperationExecutor opex;
  private final OperationContext operationCtx;
  private final QueueName queueName;
  private final QueueConsumer queueConsumer;

  public SingleQueueReader(OperationExecutor opex, OperationContext operationCtx,
                           QueueName queueName, QueueConsumer queueConsumer) {
    this.opex = opex;
    this.operationCtx = operationCtx;
    this.queueName = queueName;
    this.queueConsumer = queueConsumer;
  }

  @Override
  public InputDatum dequeue() throws OperationException {
    byte[] queueNameBytes = queueName.toBytes();
    QueueDequeue dequeue = null; // new QueueDequeue(queueNameBytes, queueConsumer, queueConsumer.getQueueConfig());
    return new InputDatum(queueConsumer, opex.execute(operationCtx, dequeue));
  }
}
