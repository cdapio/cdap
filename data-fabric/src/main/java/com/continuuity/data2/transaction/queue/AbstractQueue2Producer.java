/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.data.operation.ttqueue.QueueEntry;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
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
 * Abstract base class for {@link Queue2Producer} that emits enqueue metrics post commit.
 */
public abstract class AbstractQueue2Producer implements Queue2Producer, TransactionAware {

  private final QueueMetrics queueMetrics;
  private final BlockingQueue<QueueEntry> queue;
  private Transaction transaction;
  private int lastEnqueueCount;

  protected AbstractQueue2Producer(QueueMetrics queueMetrics) {
    this.queueMetrics = queueMetrics;
    this.queue = new LinkedBlockingQueue<QueueEntry>();
  }

  @Override
  public void enqueue(QueueEntry entry) throws IOException {
    Preconditions.checkState(transaction != null, "Enqueue called outside of transaction.");
    queue.add(entry);
  }

  @Override
  public void enqueue(Iterable <QueueEntry> entries) throws IOException {
    Preconditions.checkState(transaction != null, "Enqueue called outside of transaction.");
    Iterables.addAll(queue, entries);
  }

  @Override
  public void startTx(Transaction tx) {
    queue.clear();
    transaction = tx;
    lastEnqueueCount = 0;
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
    persist(entries, tx);
    return true;
  }

  @Override
  public void postTxCommit() {
    if (lastEnqueueCount > 0) {
      queueMetrics.emitEnqueue(lastEnqueueCount);
    }
  }

  @Override
  public boolean rollbackTx() throws Exception {
    Transaction tx = transaction;
    transaction = null;
    doRollback();
    return true;
  }

  protected abstract void persist(Iterable<QueueEntry> entries, Transaction transaction) throws Exception;

  protected abstract void doRollback() throws Exception;
}
