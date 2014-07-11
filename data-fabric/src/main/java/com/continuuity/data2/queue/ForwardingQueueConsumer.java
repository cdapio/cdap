/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.queue;

import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.Collection;

/**
 * A {@link TransactionAware} {@link QueueConsumer} that forwards all queue operations to another
 * {@link QueueConsumer} and optionally forward transaction operations if the target {@link QueueConsumer}
 * is also {@link TransactionAware}.
 */
public abstract class ForwardingQueueConsumer implements QueueConsumer, TransactionAware {

  protected final QueueConsumer consumer;
  protected final TransactionAware txAware;

  public ForwardingQueueConsumer(QueueConsumer consumer) {
    this.consumer = consumer;
    if (consumer instanceof TransactionAware) {
      txAware = (TransactionAware) consumer;
    } else {
      txAware = null;
    }
  }

  @Override
  public String getTransactionAwareName() {
    return getClass().getSimpleName() + "(" + txAware.getTransactionAwareName() + ")";
  }

  @Override
  public QueueName getQueueName() {
    return consumer.getQueueName();
  }

  @Override
  public ConsumerConfig getConfig() {
    return consumer.getConfig();
  }

  @Override
  public DequeueResult<byte[]> dequeue() throws IOException {
    return consumer.dequeue();
  }

  @Override
  public DequeueResult<byte[]> dequeue(int maxBatchSize) throws IOException {
    return consumer.dequeue(maxBatchSize);
  }

  @Override
  public void startTx(Transaction tx) {
    if (txAware != null) {
      txAware.startTx(tx);
    }
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    return (txAware != null) ? txAware.getTxChanges() : ImmutableList.<byte[]>of();
  }

  @Override
  public boolean commitTx() throws Exception {
    return (txAware == null) || txAware.commitTx();
  }

  @Override
  public void postTxCommit() {
    if (txAware != null) {
      txAware.postTxCommit();
    }
  }

  @Override
  public boolean rollbackTx() throws Exception {
    return (txAware == null) || txAware.rollbackTx();
  }
}
