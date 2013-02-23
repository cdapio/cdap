package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.procedure.ProcedureResponse;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 */
abstract class ForwardingResponseWriter implements ProcedureResponse.Writer {

  private final ProcedureResponse.Writer delegate;

  protected ForwardingResponseWriter(ProcedureResponse.Writer delegate) {
    this.delegate = delegate;
  }

  @Override
  public ProcedureResponse.Writer write(ByteBuffer buffer) throws IOException {
    return delegate.write(buffer);
  }

  @Override
  public ProcedureResponse.Writer write(byte[] bytes) throws IOException {
    return delegate.write(bytes);
  }

  @Override
  public ProcedureResponse.Writer write(byte[] bytes, int off, int len) throws IOException {
    return delegate.write(bytes, off, len);
  }

  @Override
  public ProcedureResponse.Writer write(String content) throws IOException {
    return delegate.write(content);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
