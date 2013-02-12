package com.continuuity.data.operation.ttqueue;

import com.continuuity.api.data.OperationException;

/**
*
*/
public class QueueFinalize {
  private final byte[] queueName;
  private final QueueEntryPointer entryPointer;
  private final QueueConsumer consumer;
  private final int totalNumGroups;
  public QueueFinalize(final byte[] queueName, QueueEntryPointer entryPointer, QueueConsumer consumer,
                       int totalNumGroups) {
    this.queueName = queueName;
    this.entryPointer = entryPointer;
    this.consumer = consumer;
    this.totalNumGroups = totalNumGroups;
  }

  public void execute(TTQueueTable queueTable) throws OperationException {
    queueTable.finalize(queueName, entryPointer, consumer, totalNumGroups);
  }
}
