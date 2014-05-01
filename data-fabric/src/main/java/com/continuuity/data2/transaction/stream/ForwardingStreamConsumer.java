/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.DequeueResult;
import com.continuuity.data2.transaction.Transaction;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * A {@link StreamConsumer} that forwards every methods to another {@link StreamConsumer}.
 */
public abstract class ForwardingStreamConsumer implements StreamConsumer {

  private final StreamConsumer delegate;

  protected ForwardingStreamConsumer(StreamConsumer delegate) {
    this.delegate = delegate;
  }

  @Override
  public QueueName getStreamName() {
    return delegate.getStreamName();
  }

  @Override
  public ConsumerConfig getConsumerConfig() {
    return delegate.getConsumerConfig();
  }

  @Override
  public DequeueResult<StreamEvent> poll(int maxEvents, long timeout,
                                         TimeUnit timeoutUnit) throws IOException, InterruptedException {
    return delegate.poll(maxEvents, timeout, timeoutUnit);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public void startTx(Transaction tx) {
    delegate.startTx(tx);
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    return delegate.getTxChanges();
  }

  @Override
  public boolean commitTx() throws Exception {
    return delegate.commitTx();
  }

  @Override
  public void postTxCommit() {
    delegate.postTxCommit();
  }

  @Override
  public boolean rollbackTx() throws Exception {
    return delegate.rollbackTx();
  }

  @Override
  public String getName() {
    return delegate.getName();
  }
}
