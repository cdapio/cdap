package com.continuuity.internal.app.queue;

import com.continuuity.api.data.OperationException;
import com.continuuity.app.program.Program;
import com.continuuity.app.queue.InputDatum;
import com.continuuity.app.queue.QueueName;
import com.continuuity.app.queue.QueueReader;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.QueueDequeue;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

/**
 *
 */
public final class SingleQueueReader implements QueueReader {

  private final OperationExecutor opex;
  private final OperationContext operationCtx;
  private final QueueName queueName;
  private final Supplier<QueueConsumer> queueConsumer;
  private final int numGroups;

  @Inject
  public SingleQueueReader(OperationExecutor opex,
                           @Assisted Program program,
                           @Assisted QueueName queueName,
                           @Assisted Supplier<QueueConsumer> queueConsumer,
                           @Assisted int numGroups) {
    this.opex = opex;
    this.operationCtx = new OperationContext(program.getAccountId(), program.getApplicationId());
    this.queueName = queueName;
    this.queueConsumer = queueConsumer;
    this.numGroups = numGroups;
  }

  @Override
  public InputDatum dequeue() throws OperationException {
    QueueConsumer consumer = queueConsumer.get();
    byte[] queueNameBytes = queueName.toBytes();
    QueueDequeue dequeue = new QueueDequeue(queueNameBytes, consumer, consumer.getQueueConfig());
    return new QueueInputDatum(consumer, queueName, opex.execute(operationCtx, dequeue), numGroups);
  }
}
