/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data2.transaction.queue.inmemory;

import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.queue.QueueEntry;
import co.cask.cdap.data2.transaction.queue.AbstractQueueProducer;
import co.cask.cdap.data2.transaction.queue.QueueMetrics;
import org.apache.tephra.Transaction;

/**
 * Producer for an in-memory queue.
 */
public class InMemoryQueueProducer extends AbstractQueueProducer {

  private final QueueName queueName;
  private final InMemoryQueueService queueService;
  private int lastEnqueueCount;
  private Transaction commitTransaction;

  public InMemoryQueueProducer(QueueName queueName, InMemoryQueueService queueService, QueueMetrics queueMetrics) {
    super(queueMetrics, queueName);
    this.queueName = queueName;
    this.queueService = queueService;
  }

  private InMemoryQueue getQueue() {
    return queueService.getQueue(queueName);
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

    InMemoryQueue queue = getQueue();
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
      InMemoryQueue queue = getQueue();
      for (int seqId = 0; seqId < lastEnqueueCount; seqId++) {
        queue.undoEnqueue(commitTransaction.getWritePointer(), seqId);
      }
    }
  }
}
