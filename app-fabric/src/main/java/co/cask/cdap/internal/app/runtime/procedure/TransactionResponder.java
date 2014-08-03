/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.internal.app.runtime.procedure;

import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;
import com.continuuity.tephra.TransactionContext;
import com.continuuity.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link ProcedureResponse} that handle transaction finish/abort and forward output requests
 * to a delegated {@link ProcedureResponder}.
 */
final class TransactionResponder extends AbstractProcedureResponder {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionResponder.class);

  private final TransactionContext txContext;
  private final ProcedureResponder responder;
  private ProcedureResponse.Writer writer;

  TransactionResponder(TransactionContext txContext, ProcedureResponder responder) {
    this.txContext = txContext;
    this.responder = responder;
  }

  public synchronized void close() throws IOException {
    if (writer != null) {
      writer.close();
    }
  }

  @Override
  public synchronized ProcedureResponse.Writer stream(ProcedureResponse response) throws IOException {
    if (writer != null) {
      return writer;
    }

    try {
      writer = new TransactionWriter(responder.stream(response), txContext);
    } catch (Throwable t) {
      writer = ResponseWriters.CLOSED_WRITER;
      throw propagate(t);
    }
    return writer;
  }

  @Override
  public synchronized void sendJson(ProcedureResponse response, Object object) throws IOException {
    if (writer != null) {
      throw new IOException("A writer is already opened for streaming or the response was already sent.");
    }

    // Commit the transaction and send out the json.
    try {
      txContext.finish();
      responder.sendJson(response, object);
    } catch (Throwable t) {
      throw propagate(t);
    } finally {
      writer = ResponseWriters.CLOSED_WRITER;
    }
  }

  @Override
  public synchronized void error(ProcedureResponse.Code errorCode, String errorMessage) throws IOException {
    if (writer != null) {
      throw new IOException("A writer is already opened for streaming or the response was already sent.");
    }

    // TODO: Should we abort?
    // Abort the transaction and send out error.
    try {
      txContext.abort();
      responder.error(errorCode, errorMessage);
    } catch (Throwable t) {
      throw propagate(t);
    } finally {
      writer = ResponseWriters.CLOSED_WRITER;
    }
  }

  private IOException propagate(Throwable t) throws IOException {
    if (t instanceof IOException) {
      throw (IOException) t;
    } else {
      throw new IOException(t);
    }
  }

  private static final class TransactionWriter implements ProcedureResponse.Writer {

    private final ProcedureResponse.Writer delegate;
    private final TransactionContext txContext;
    private final AtomicBoolean closed;

    private TransactionWriter(ProcedureResponse.Writer delegate, TransactionContext txContext) {
      this.delegate = delegate;
      this.txContext = txContext;
      this.closed = new AtomicBoolean(false);
    }

    @Override
    public ProcedureResponse.Writer write(ByteBuffer buffer) throws IOException {
      try {
        delegate.write(buffer);
        return this;
      } catch (IOException e) {
        try {
          txContext.abort();
          LOG.info("Transaction aborted due to IOException", e);
        } catch (TransactionFailureException te) {
          LOG.error("Fail to abort transaction.", te);
        }
        throw e;
      }
    }

    @Override
    public ProcedureResponse.Writer write(byte[] bytes) throws IOException {
      try {
        delegate.write(bytes);
        return this;
      } catch (IOException e) {
        try {
          txContext.abort();
          LOG.info("Transaction aborted due to IOException", e);
        } catch (TransactionFailureException te) {
          LOG.error("Fail to abort transaction.", te);
        }
        throw e;
      }
    }

    @Override
    public ProcedureResponse.Writer write(byte[] bytes, int off, int len) throws IOException {
      try {
        delegate.write(bytes, off, len);
        return this;
      } catch (IOException e) {
        try {
          txContext.abort();
          LOG.info("Transaction aborted due to IOException", e);
        } catch (TransactionFailureException te) {
          LOG.error("Fail to abort transaction.", te);
        }
        throw e;
      }
    }

    @Override
    public ProcedureResponse.Writer write(String content) throws IOException {
      try {
        delegate.write(content);
        return this;
      } catch (IOException e) {
        try {
          txContext.abort();
          LOG.info("Transaction aborted due to IOException", e);
        } catch (TransactionFailureException te) {
          LOG.error("Fail to abort transaction.", te);
        }
        throw e;
      }
    }

    @Override
    public void close() throws IOException {
      if (!closed.compareAndSet(false, true)) {
        return;
      }
      try {
        delegate.close();
        txContext.finish();
      } catch (IOException e) {
        try {
          txContext.abort();
          LOG.info("Transaction aborted due to IOException", e);
        } catch (TransactionFailureException te) {
          LOG.error("Fail to abort transaction.", te);
        }
        throw e;
      } catch (TransactionFailureException te) {
        LOG.info("Transaction finish failed.", te);
      }
    }
  }
}
