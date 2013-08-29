package com.continuuity.data2.transaction.queue.inmemory;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.queue.AbstractQueue2Producer;
import com.continuuity.data2.transaction.queue.QueueMetrics;

/**
 * Producer for an in-memory queue.
 */
public class InMemoryQueue2Producer extends AbstractQueue2Producer {

  private final InMemoryQueue queue;
  private int lastEnqueueCount;
  private Transaction commitTransaction;

  public InMemoryQueue2Producer(QueueName queueName, InMemoryQueueService queueService, QueueMetrics queueMetrics) {
    super(queueMetrics);
    this.queue = queueService.getQueue(queueName);
  }

  @Override
  public void startTx(Transaction tx) {
    super.startTx(tx);
    commitTransaction = null;
  }

  @Override
  protected int persist(Iterable<QueueEntry> entries, Transaction transaction) throws Exception {
    commitTransaction = transaction;
    int seqId = 0;
    int bytes = 0;

    for (QueueEntry entry : entries) {
      queue.enqueue(transaction.getWritePointer(), seqId++, entry);
      bytes += entry.getData().length;
    }
    lastEnqueueCount = seqId;
    return bytes;
  }

  @Override
  protected void doRollback() {
    if (commitTransaction != null) {
      for (int seqId = 0; seqId < lastEnqueueCount; seqId++) {
        queue.undoEnqueue(commitTransaction.getWritePointer(), seqId);
      }
    }
  }
}
