package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.executor.omid.StatefulQueueOperationExecutor;

/**
*
*/
public class QueueFinalize {
  private final byte[] queueName;
  private final QueueEntryPointer [] entryPointers;
  private final StatefulQueueOperationExecutor statefulQueueOperationExecutor;
  private final QueueConsumer consumer;
  private final int totalNumGroups;

  public QueueFinalize(final byte[] queueName, QueueEntryPointer[] entryPointers,
                       StatefulQueueOperationExecutor statefulQueueOperationExecutor, QueueConsumer consumer,
                       int totalNumGroups) {
    this.queueName = queueName;
    this.entryPointers = entryPointers;
    this.statefulQueueOperationExecutor = statefulQueueOperationExecutor;
    this.consumer = consumer;
    this.totalNumGroups = totalNumGroups;
  }

  public byte[] getQueueName() {
    return queueName;
  }

  public void execute(final TTQueueTable queueTable, final long writePoint) throws OperationException {
    statefulQueueOperationExecutor.run(queueName, consumer,
                                       new StatefulQueueOperationExecutor.QueueRunnable() {
                                         @Override
                                         public void run(StatefulQueueConsumer statefulQueueConsumer)
                                           throws OperationException {
                                           queueTable.finalize(queueName, entryPointers, statefulQueueConsumer,
                                                               totalNumGroups, writePoint);
                                         }
                                       });
  }
}
