package com.continuuity.data2.transaction.queue;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data2.transaction.Transaction;

/**
 * Producer for an in-memory queue.
 */
public class InMemoryQueue2Producer extends AbstractQueue2Producer {

  private final InMemoryQueue queue;
  private long lastWritePointer;
  private int lastEnqueueCount;

  public InMemoryQueue2Producer(QueueName queueName, InMemoryQueueService queueService, QueueMetrics queueMetrics) {
    super(queueMetrics);
    this.queue = queueService.getQueue(queueName);
  }

  @Override
  public boolean rollbackTx() throws Exception {
    for (int seqId = 0; seqId < lastEnqueueCount; seqId++) {
      queue.undoEnqueue(lastWritePointer, seqId);
    }
    return true;
  }

  @Override
  protected void persist(Iterable<QueueEntry> entries, Transaction transaction) throws Exception {
    lastWritePointer = transaction.getWritePointer();

    int seqId = 0;
    for (QueueEntry entry : entries) {
      queue.enqueue(lastWritePointer, seqId++, entry);
    }
    lastEnqueueCount = seqId;
  }
}
