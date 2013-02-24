package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.procedure.ProcedureResponse;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link ProcedureResponse.Writer} that is always closed. all write operations will throw {@link IOException}.
 */
final class ClosedResponseWriter implements ProcedureResponse.Writer {

  @Override
  public ProcedureResponse.Writer write(ByteBuffer buffer) throws IOException {
    throw new IOException("Writer already closed");
  }

  @Override
  public ProcedureResponse.Writer write(byte[] bytes) throws IOException {
    throw new IOException("Writer already closed");
  }

  @Override
  public ProcedureResponse.Writer write(byte[] bytes, int off, int len) throws IOException {
    throw new IOException("Writer already closed");
  }

  @Override
  public ProcedureResponse.Writer write(String content) throws IOException {
    throw new IOException("Writer already closed");
  }

  @Override
  public void close() throws IOException {
    // no-op
  }
}
