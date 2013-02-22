package com.continuuity.internal.app.queue;

import com.continuuity.api.data.OperationException;
import com.continuuity.app.queue.QueueName;
import com.continuuity.app.queue.QueueReader;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.continuuity.app.queue.InputDatum;
import com.google.common.base.Supplier;

/**
 *
 */
public final class SingleQueueReader implements QueueReader {

  private final OperationExecutor opex;
  private final OperationContext operationCtx;
  private final QueueName queueName;
  private final Supplier<QueueConsumer> queueConsumer;

  public SingleQueueReader(OperationExecutor opex, OperationContext operationCtx,
                           QueueName queueName, Supplier<QueueConsumer> queueConsumer) {
    this.opex = opex;
    this.operationCtx = operationCtx;
    this.queueName = queueName;
    this.queueConsumer = queueConsumer;
  }

  @Override
  public InputDatum dequeue() throws OperationException {
    QueueConsumer consumer = queueConsumer.get();
    byte[] queueNameBytes = queueName.toBytes();
    QueueDequeue dequeue = new QueueDequeue(queueNameBytes, consumer, consumer.getQueueConfig());
    return new QueueInputDatum(consumer, queueName, opex.execute(operationCtx, dequeue));
  }
}
