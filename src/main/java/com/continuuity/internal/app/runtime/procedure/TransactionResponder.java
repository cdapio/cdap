package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.data.operation.executor.TransactionAgent;

import java.io.IOException;

/**
*
*/
final class TransactionResponder implements ProcedureResponder {

  private final TransactionAgent txAgent;
  private final ProcedureResponder responder;
  private ProcedureResponse.Writer writer;

  TransactionResponder(TransactionAgent txAgent, ProcedureResponder responder) {
    this.txAgent = txAgent;
    this.responder = responder;
  }

  @Override
  public synchronized ProcedureResponse.Writer stream(ProcedureResponse response) throws IOException {
    if (writer != null) {
      return writer;
    }

    // Commit the transaction and create a new writer
    try {
      txAgent.finish();
      writer = responder.stream(response);
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
      txAgent.finish();
      responder.sendJson(response, object);
    } catch (Throwable t) {
      writer = ResponseWriters.CLOSED_WRITER;
      throw propagate(t);
    }
  }

  private IOException propagate(Throwable t) throws IOException {
    if (t instanceof IOException) {
      throw (IOException)t;
    } else {
      throw new IOException(t);
    }
  }
}
