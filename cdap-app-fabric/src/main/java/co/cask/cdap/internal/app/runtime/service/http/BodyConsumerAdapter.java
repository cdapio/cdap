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
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.service.http.HttpContentConsumer;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.http.BodyConsumer;
import co.cask.http.HttpResponder;
import co.cask.tephra.TransactionConflictException;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import org.apache.twill.common.Cancellable;
import org.jboss.netty.buffer.ChannelBuffer;

import java.nio.ByteBuffer;
import javax.annotation.Nullable;

/**
 * An adapter class to delegate calls from {@link BodyConsumer} to {@link HttpContentConsumer}.
 */
final class BodyConsumerAdapter extends BodyConsumer {

  private final DelayedHttpServiceResponder responder;
  private final HttpContentConsumer delegate;
  private final TransactionContext txContext;
  private final TransactionalHttpServiceContext serviceContext;
  private final Cancellable contextReleaser;
  private final Transactional transactional;
  private final ClassLoader programContextClassLoader;

  private boolean completed;

  /**
   * Constructs a new instance.
   *
   * @param responder the responder used for sending response back to client
   * @param delegate the {@link HttpContentConsumer} to delegate calls to
   * @param txContext a {@link TransactionContext} for executing transactional task
   * @param serviceContext the {@link TransactionalHttpServiceContext} for this handler.
   * @param contextReleaser A {@link Cancellable} for returning the context back to the http server
   */
  BodyConsumerAdapter(DelayedHttpServiceResponder responder, HttpContentConsumer delegate,
                      TransactionContext txContext, TransactionalHttpServiceContext serviceContext,
                      Cancellable contextReleaser) {
    this.responder = responder;
    this.delegate = delegate;
    this.txContext = txContext;
    this.serviceContext = serviceContext;
    this.contextReleaser = contextReleaser;
    this.transactional = createTransactional(this.txContext);
    this.programContextClassLoader = new CombineClassLoader(null, ImmutableList.of(delegate.getClass().getClassLoader(),
                                                                                   getClass().getClassLoader()));
  }

  @Override
  public void chunk(final ChannelBuffer request, HttpResponder responder) {
    // Due to async nature of netty, chunk might get called even we try to close the connection in onError.
    if (completed) {
      return;
    }

    try {
      final ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(programContextClassLoader);
      try {
        delegate.onReceived(request.toByteBuffer(), transactional);
      } finally {
        ClassLoaders.setContextClassLoader(oldClassLoader);
      }
    } catch (Throwable t) {
      onError(t, this.responder);
    }
  }

  @Override
  public void finished(HttpResponder responder) {
    try {
      txExecute(txContext, new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          delegate.onFinish(BodyConsumerAdapter.this.responder);
        }
      });
    } catch (Throwable t) {
      onError(t, this.responder);
      return;
    }

    // To the HttpContentConsumer, the call is completed even if it fails to send response back to client.
    completed = true;
    try {
      serviceContext.dismissTransactionContext();
      BodyConsumerAdapter.this.responder.execute();
    } finally {
      contextReleaser.cancel();
    }
  }

  @Override
  public void handleError(final Throwable cause) {
    // When this method is called from netty-http, the response has already been sent, hence uses a no-op
    // DelayedHttpServiceResponder for the onError call.
    onError(cause, new DelayedHttpServiceResponder(responder) {
      @Override
      protected void doSend(int status, ByteBuffer content, String contentType,
                            @Nullable Multimap<String, String> headers, boolean copy) {
        // no-op
      }

      @Override
      public void setTransactionFailureResponse(Throwable t) {
        // no-op
      }

      @Override
      public void execute() {
        // no-op
      }
    });
  }

  /**
   * Calls the {@link HttpContentConsumer#onError(HttpServiceResponder, Throwable)} method from a transaction.
   */
  private void onError(final Throwable cause, final DelayedHttpServiceResponder responder) {
    if (completed) {
      return;
    }

    // To the HttpContentConsumer, once onError is called, no other methods will be triggered
    completed = true;
    try {
      txExecute(txContext, new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          delegate.onError(responder, cause);
        }
      });
    } catch (Throwable t) {
      responder.setTransactionFailureResponse(t);
    } finally {
      try {
        serviceContext.dismissTransactionContext();
        responder.execute(false);
      } finally {
        contextReleaser.cancel();
      }
    }
  }

  private Transactional createTransactional(final TransactionContext txContext) {
    return new Transactional() {
      @Override
      public void execute(final TxRunnable runnable) throws TransactionFailureException {
        // This method is only called from user code, hence the context classloader is the
        // program context classloader (or whatever the user set to).
        // We need to switch the classloader back to CDAP system classloader before calling txExecute
        // since it starting of transaction should happens in CDAP system context, not program context
        ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(getClass().getClassLoader());
        try {
          txExecute(txContext, runnable);
        } finally {
          ClassLoaders.setContextClassLoader(oldClassLoader);
        }
      }
    };
  }

  /**
   * Executes the given {@link TxRunnable} in a transaction. The transaction lifecycle is managed by the
   * given {@link TransactionContext} instance.
   *
   * @throws TransactionFailureException if execution failed
   * @throws TransactionConflictException if conflict is detected when trying to commit the transaction
   */
  private void txExecute(TransactionContext txContext, TxRunnable runnable) throws TransactionFailureException {
    txContext.start();
    try {
      ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(programContextClassLoader);
      try {
        runnable.run(serviceContext);
      } finally {
        ClassLoaders.setContextClassLoader(oldClassLoader);
      }
    } catch (Throwable t) {
      txContext.abort(new TransactionFailureException("Exception raised from TxRunnable.run()", t));
    }
    txContext.finish();
  }
}
