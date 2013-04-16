package com.continuuity.internal.app.queue;

import com.continuuity.api.data.OperationException;
import com.continuuity.app.program.Program;
import com.continuuity.app.queue.QueueName;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueAdmin;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.StatefulQueueConsumer;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

/**
 *  Creates a StatefulQueueConsumer
 */
public class StatefulQueueConsumerFactory implements QueueConsumerFactory {
  private final OperationExecutor opex;
  private final OperationContext operationCtx;
  private final int instanceId;
  private final long groupId;
  private final String groupName;
  private final QueueConfig queueConfig;
  private final QueueName queueName;

  @Inject
  public StatefulQueueConsumerFactory(OperationExecutor opex, @Assisted Program program, @Assisted int instanceId,
                                      @Assisted long groupId, @Assisted String groupName,
                                      @Assisted QueueConfig queueConfig, @Assisted QueueName queueName) {
    this.opex = opex;
    this.operationCtx = new OperationContext(program.getAccountId(), program.getApplicationId());
    this.instanceId = instanceId;
    this.groupId = groupId;
    this.groupName = groupName;
    this.queueConfig = queueConfig;
    this.queueName = queueName;
  }

  /**
   * Creates a StatefulQueueConsumer with the given groupSize, runs a QueueConfigure with the new StatefulQueueConsumer.
   * @param groupSize Size of the group of which the created StatefulQueueConsumer will be part of
   * @return Created StatefulQueueConsumer
   */
  @Override
  public QueueConsumer create(int groupSize) {
    StatefulQueueConsumer queueConsumer =
      new StatefulQueueConsumer(instanceId, groupId, groupSize, groupName, queueConfig);

    // configure the queue
    try {
      opex.execute(operationCtx, null, new QueueAdmin.QueueConfigure(queueName.toBytes(), queueConsumer));
    } catch (OperationException e) {
      // There is nothing much that can be done to resolve this OperationException, propagate it as a runtime exception
      Throwables.propagate(e);
    }

    return queueConsumer;
  }
}
