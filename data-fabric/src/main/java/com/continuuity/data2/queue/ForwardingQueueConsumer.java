/*
 * Copyright 2012-2014 Continuuity, Inc.
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
package com.continuuity.data2.queue;

import com.continuuity.common.queue.QueueName;
import com.continuuity.tephra.Transaction;
import com.continuuity.tephra.TransactionAware;
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
