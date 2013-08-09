package com.continuuity.internal.app.queue;

import com.continuuity.api.data.OperationException;
import com.continuuity.app.program.Program;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueConfig;
import com.continuuity.data.operation.ttqueue.QueueConsumer;
import com.continuuity.data.operation.ttqueue.admin.QueueConfigure;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;

/**
 *  Creates a QueueConsumer.
 */
public class QueueConsumerFactoryImpl implements QueueConsumerFactory {
  private final OperationExecutor opex;
  private final OperationContext operationCtx;
  private final int instanceId;
  private final long groupId;
  private final String groupName;
  private final QueueName queueName;
  private final QueueInfo queueInfo;
  private final boolean sync;

  @Inject
  public QueueConsumerFactoryImpl(OperationExecutor opex, @Assisted Program program, @Assisted int instanceId,
                                  @Assisted long groupId, @Assisted String groupName, @Assisted QueueName queueName,
                                  @Assisted QueueInfo queueInfo, @Assisted boolean singleEntry) {
    this.opex = opex;
    this.operationCtx = new OperationContext(program.getAccountId(), program.getApplicationId());
    this.instanceId = instanceId;
    this.groupId = groupId;
    this.groupName = groupName;
    this.queueName = queueName;
    this.queueInfo = queueInfo;
    this.sync = singleEntry;
  }

  /**
   * Creates a QueueConsumer with the given groupSize, runs a QueueConfigure with the new QueueConsumer.
   * @param groupSize Size of the group of which the created QueueConsumer will be part of
   * @return Created QueueConsumer
   */
  @Override
  public QueueConsumer create(int groupSize) {
    QueueConfig queueConfig;

    if (queueInfo.isBatchMode()) {
      queueConfig =
        new QueueConfig(queueInfo.getPartitionerType(), sync, queueInfo.getBatchSize(), queueInfo.isBatchMode());
    } else {
      queueConfig = new QueueConfig(queueInfo.getPartitionerType(), sync);
    }

    QueueConsumer queueConsumer = new QueueConsumer(instanceId, groupId, groupSize, groupName,
                                                    queueInfo.getPartitionKey(), queueConfig);

    // configure the queue
    try {
      opex.execute(operationCtx, new QueueConfigure(queueName.toBytes(), queueConsumer));
    } catch (OperationException e) {
      // There is nothing much that can be done to resolve this OperationException, propagate it as a runtime exception
      throw Throwables.propagate(e);    // Using throw to suppress warning
    }

    return queueConsumer;
  }
}
