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

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.service.http.HttpContentProducer;
import co.cask.cdap.common.lang.ClassLoaders;
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
  private final ClassLoader programContextClassloader;
  private final Transactional transactional;
  private final Cancellable contextReleaser;
  private final TransactionControl onFinishTxControl;
  private final TransactionControl onErrorTxControl;

  private boolean completed;

  BodyProducerAdapter(HttpContentProducer delegate, Transactional transactional,
                      ClassLoader programContextClassLoader, Cancellable contextReleaser,
                      TransactionControl defaultTxControl) {
    this.delegate = delegate;
    this.programContextClassloader = programContextClassLoader;
    this.transactional = transactional;
    this.contextReleaser = contextReleaser;
    this.onFinishTxControl = Transactions.getTransactionControl(defaultTxControl, HttpContentProducer.class,
                                                                delegate, "onFinish");
    this.onErrorTxControl = Transactions.getTransactionControl(defaultTxControl, HttpContentProducer.class,
                                                               delegate, "onError", Throwable.class);
  }

  @Override
  public long getContentLength() {
    ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(programContextClassloader);
    try {
      return delegate.getContentLength();
    } finally {
      ClassLoaders.setContextClassLoader(oldClassLoader);
    }
  }

  @Override
  public ByteBuf nextChunk() throws Exception {
    ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(programContextClassloader);
    try {
      return Unpooled.copiedBuffer(delegate.nextChunk(transactional));
    } finally {
      ClassLoaders.setContextClassLoader(oldClassLoader);
    }
  }

  @Override
  public void finished() throws Exception {
    if (TransactionControl.IMPLICIT == onFinishTxControl) {
      transactional.execute(context -> delegate.onFinish());
    } else {
      delegate.onFinish();
    }

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
      if (TransactionControl.IMPLICIT == onErrorTxControl) {
        transactional.execute(context -> delegate.onError(throwable));
      } else {
        delegate.onError(throwable);
      }
    } catch (Throwable t) {
      throwable.addSuppressed(t);
      // nothing much can be done. Simply emit a debug log.
      LOG.warn("Exception in calling HttpContentProducer.onError.", t);
    }
    contextReleaser.cancel();
  }
}
