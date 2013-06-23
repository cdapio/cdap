package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.queueproxy.QueueRunnable;
import com.continuuity.data.operation.executor.omid.queueproxy.QueueStateProxy;

/**
*
*/
public class QueueFinalize {
  private final byte[] queueName;
  private final QueueEntryPointer [] entryPointers;
  private final QueueConsumer consumer;
  private final int totalNumGroups;

  public QueueFinalize(final byte[] queueName, QueueEntryPointer[] entryPointers,
                       QueueConsumer consumer,
                       int totalNumGroups) {
    this.queueName = queueName;
    this.entryPointers = entryPointers;
    this.consumer = consumer;
    this.totalNumGroups = totalNumGroups;
  }

  public byte[] getQueueName() {
    return queueName;
  }

  public void execute(final QueueStateProxy queueStateProxy, final TTQueueTable queueTable,
                      final Transaction transaction)
    throws OperationException {
    queueStateProxy.run(queueName, consumer,
                                       new QueueRunnable() {
                                         @Override
                                         public void run(StatefulQueueConsumer statefulQueueConsumer)
                                           throws OperationException {
                                           queueTable.finalize(queueName, entryPointers, statefulQueueConsumer,
                                                               totalNumGroups, transaction);
                                         }
                                       });
  }
}
