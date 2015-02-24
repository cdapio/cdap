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
package co.cask.cdap.data2.transaction.queue;

import co.cask.cdap.common.queue.QueueName;
import co.cask.cdap.data2.queue.QueueEntry;
import co.cask.cdap.data2.queue.QueueProducer;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Abstract base class for {@link QueueProducer} that emits enqueue metrics post commit.
 */
public abstract class AbstractQueueProducer implements QueueProducer, TransactionAware {

  private final QueueMetrics queueMetrics;
  private final BlockingQueue<QueueEntry> queue;
  private final QueueName queueName;
  private Transaction transaction;
  private int lastEnqueueCount;
  private int lastEnqueueBytes;

  protected AbstractQueueProducer(QueueMetrics queueMetrics, QueueName queueName) {
    this.queueMetrics = queueMetrics;
    this.queue = new LinkedBlockingQueue<QueueEntry>();
    this.queueName = queueName;
  }

  @Override
  public String getTransactionAwareName() {
    return getClass().getSimpleName() + "(queue = " + queueName + ")";
  }

  @Override
  public void enqueue(QueueEntry entry) throws IOException {
    Preconditions.checkState(transaction != null, "Enqueue called outside of transaction.");
    queue.add(entry);
  }

  @Override
  public void enqueue(Iterable<QueueEntry> entries) throws IOException {
    Preconditions.checkState(transaction != null, "Enqueue called outside of transaction.");
    Iterables.addAll(queue, entries);
  }

  @Override
  public void startTx(Transaction tx) {
    queue.clear();
    transaction = tx;
    lastEnqueueCount = 0;
    lastEnqueueBytes = 0;
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    // Always empty changes, as enqueue is append only, nothing could be conflict.
    return ImmutableList.of();
  }

  @Override
  public boolean commitTx() throws Exception {
    Preconditions.checkState(transaction != null, "Commit without starting transaction.");
    Transaction tx = transaction;
    transaction = null;
    List<QueueEntry> entries = Lists.newArrayListWithCapacity(queue.size());
    queue.drainTo(entries);
    lastEnqueueCount = entries.size();
    lastEnqueueBytes = persist(entries, tx);
    return true;
  }

  @Override
  public void postTxCommit() {
    if (lastEnqueueCount > 0) {
      queueMetrics.emitEnqueue(lastEnqueueCount);
      queueMetrics.emitEnqueueBytes(lastEnqueueBytes);
    }
  }

  @Override
  public boolean rollbackTx() throws Exception {
    Transaction tx = transaction;
    transaction = null;
    doRollback();
    return true;
  }

  @Override
  public void close() throws IOException {
    // No-op
  }

  /**
   * Persists queue entries.
   * @param entries queue entries to persist.
   * @param transaction transaction to use.
   * @return size in bytes of the entries persisted.
   * @throws Exception
   */
  protected abstract int persist(Iterable<QueueEntry> entries, Transaction transaction) throws Exception;

  protected abstract void doRollback() throws Exception;
}
