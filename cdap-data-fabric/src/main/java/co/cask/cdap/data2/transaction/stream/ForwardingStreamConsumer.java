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
package co.cask.cdap.data2.transaction.stream;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.data2.queue.ConsumerConfig;
import co.cask.cdap.data2.queue.DequeueResult;
import co.cask.cdap.proto.Id;
import co.cask.tephra.Transaction;

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
  public Id.Stream getStreamName() {
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
  public String getTransactionAwareName() {
    return delegate.getTransactionAwareName();
  }
}
