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

import co.cask.cdap.api.procedure.ProcedureResponse;

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
