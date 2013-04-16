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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

import java.util.List;
import java.util.Map;

/**
 *
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

  @Override
  public QueueConsumer create(int groupSize) throws OperationException {
      StatefulQueueConsumer queueConsumer =
        new StatefulQueueConsumer(instanceId, groupId, groupSize, groupName, queueConfig);

      // configure the queue
      opex.execute(operationCtx, null, new QueueAdmin.QueueConfigure(queueName.toBytes(), queueConsumer));

    return queueConsumer;
  }
}
