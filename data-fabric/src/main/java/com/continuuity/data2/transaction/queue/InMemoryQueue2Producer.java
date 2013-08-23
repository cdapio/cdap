package com.continuuity.data2.transaction.queue;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data2.transaction.Transaction;

/**
 * Producer for an in-memory queue.
 */
public class InMemoryQueue2Producer extends AbstractQueue2Producer {

  private final InMemoryQueue queue;
  private int lastEnqueueCount;

  public InMemoryQueue2Producer(QueueName queueName, InMemoryQueueService queueService, QueueMetrics queueMetrics) {
    super(queueMetrics);
    this.queue = queueService.getQueue(queueName);
  }

  @Override
  protected void persist(Iterable<QueueEntry> entries, Transaction transaction) throws Exception {
    int seqId = 0;
    for (QueueEntry entry : entries) {
      queue.enqueue(transaction.getWritePointer(), seqId++, entry);
    }
    lastEnqueueCount = seqId;
  }

  @Override
  protected void doRollback(Transaction transaction) {
    for (int seqId = 0; seqId < lastEnqueueCount; seqId++) {
      queue.undoEnqueue(transaction.getWritePointer(), seqId);
    }
  }
}
