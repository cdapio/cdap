/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.service.http;

import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.service.http.HttpContentProducer;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.http.BodyProducer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An adapter class to delegate calls from {@link HttpContentProducer} to {@link BodyProducer}
 */
final class BodyProducerAdapter extends BodyProducer {

  private static final Logger LOG = LoggerFactory.getLogger(BodyProducerAdapter.class);

  private final HttpContentProducer delegate;
  private final ServiceTaskExecutor taskExecutor;
  private final Cancellable contextReleaser;
  private final boolean useTxOnFinish;
  private final boolean useTxOnError;

  private boolean completed;

  BodyProducerAdapter(HttpContentProducer delegate, ServiceTaskExecutor taskExecutor,
                      Cancellable contextReleaser, TransactionControl defaultTxControl) {
    this.delegate = delegate;
    this.taskExecutor = taskExecutor;
    this.contextReleaser = contextReleaser;
    this.useTxOnFinish = Transactions.getTransactionControl(
      defaultTxControl, HttpContentProducer.class, delegate, "onFinish") == TransactionControl.IMPLICIT;
    this.useTxOnError = Transactions.getTransactionControl(
      defaultTxControl, HttpContentProducer.class, delegate, "onError", Throwable.class) == TransactionControl.IMPLICIT;
  }

  @Override
  public long getContentLength() {
    try {
      return taskExecutor.execute(delegate::getContentLength, false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ByteBuf nextChunk() throws Exception {
    return taskExecutor.execute(() -> Unpooled.copiedBuffer(delegate.nextChunk(taskExecutor.getTransactional())),
                                false);
  }

  @Override
  public void finished() throws Exception {
    taskExecutor.execute(delegate::onFinish, useTxOnFinish);

    try {
      contextReleaser.cancel();
    } finally {
      completed = true;
    }
  }

  @Override
  public void handleError(Throwable throwable) {
    if (completed) {
      return;
    }

    // To the HttpContentProducer, if there is error, no other methods will be triggered
    completed = true;
    try {
      taskExecutor.execute(() -> delegate.onError(throwable), useTxOnError);
    } catch (Throwable t) {
      throwable.addSuppressed(t);
      // nothing much can be done. Simply emit a debug log.
      LOG.warn("Exception in calling HttpContentProducer.onError.", t);
    }
    contextReleaser.cancel();
  }
}
