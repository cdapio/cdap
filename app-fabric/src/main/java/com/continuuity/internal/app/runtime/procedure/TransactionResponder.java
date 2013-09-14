package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.data2.transaction.TransactionManager;
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

  private final TransactionManager txManager;
  private final ProcedureResponder responder;
  private ProcedureResponse.Writer writer;

  TransactionResponder(TransactionManager txManager, ProcedureResponder responder) {
    this.txManager = txManager;
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
      writer = new TransactionWriter(responder.stream(response), txManager);
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
      txManager.finish();
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
      txManager.abort();
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
    private final TransactionManager txManager;
    private final AtomicBoolean closed;

    private TransactionWriter(ProcedureResponse.Writer delegate, TransactionManager txManager) {
      this.delegate = delegate;
      this.txManager = txManager;
      this.closed = new AtomicBoolean(false);
    }

    @Override
    public ProcedureResponse.Writer write(ByteBuffer buffer) throws IOException {
      try {
        delegate.write(buffer);
        return this;
      } catch (IOException e) {
        try {
          txManager.abort();
          LOG.info("Transaction aborted due to IOException", e);
        } catch (OperationException oe) {
          LOG.error("Fail to abort transaction.", oe);
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
          txManager.abort();
          LOG.info("Transaction aborted due to IOException", e);
        } catch (OperationException oe) {
          LOG.error("Fail to abort transaction.", oe);
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
          txManager.abort();
          LOG.info("Transaction aborted due to IOException", e);
        } catch (OperationException oe) {
          LOG.error("Fail to abort transaction.", oe);
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
          txManager.abort();
          LOG.info("Transaction aborted due to IOException", e);
        } catch (OperationException oe) {
          LOG.error("Fail to abort transaction.", oe);
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
        txManager.finish();
      } catch (IOException e) {
        try {
          txManager.abort();
          LOG.info("Transaction aborted due to IOException", e);
        } catch (OperationException oe) {
          LOG.error("Fail to abort transaction.", oe);
        }
        throw e;
      } catch (OperationException e) {
        LOG.info("Transaction finish failed.", e);
      }
    }
  }
}
