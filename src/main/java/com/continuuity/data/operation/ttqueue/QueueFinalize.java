package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;

/**
*
*/
public class QueueFinalize {
  private final byte[] queueName;
  private final QueueEntryPointer [] entryPointers;
  private final QueueConsumer consumer;
  private final int totalNumGroups;

  public QueueFinalize(final byte[] queueName, QueueEntryPointer[] entryPointers, QueueConsumer consumer,
                       int totalNumGroups) {
    this.queueName = queueName;
    this.entryPointers = entryPointers;
    this.consumer = consumer;
    this.totalNumGroups = totalNumGroups;
  }

  public byte[] getQueueName() {
    return queueName;
  }

  public void execute(TTQueueTable queueTable, long writePoint) throws OperationException {
    queueTable.finalize(queueName, entryPointers[0], consumer, totalNumGroups, writePoint);
  }
}
