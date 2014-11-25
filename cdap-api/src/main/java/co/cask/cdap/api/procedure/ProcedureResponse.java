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

package co.cask.cdap.api.procedure;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class represents the response from a {@link Procedure}.
 *
 * @deprecated As of version 2.6.0, with no direct replacement, see {@link co.cask.cdap.api.service.Service}
 */
@Deprecated
public final class ProcedureResponse {

  /**
   * Interface for writing response data.
   *
   * @deprecated As of version 2.6.0, with no direct replacement, see {@link co.cask.cdap.api.service.Service}
   */
  @Deprecated
  public interface Writer extends Closeable {

    /**
     * Writes the content of the given buffer to the response.
     * @param buffer {@link ByteBuffer} holding the content to be written. After this method
     *               returns, the {@link ByteBuffer} is drained.
     * @return The same {@link Writer} instance for writing more data.
     * @throws IOException When there is an error while writing.
     */
    Writer write(ByteBuffer buffer) throws IOException;

    /**
     * Writes the given byte array to the response. Same as calling
     * {@link #write(byte[], int, int) write(bytes, 0, bytes.length)}.
     *
     * @param bytes bytes to be written out.
     * @return The same {@link Writer} instance for writing more data.
     * @throws IOException When there is an error while writing.
     */
    Writer write(byte[] bytes) throws IOException;

    /**
     * Writes the given {@code len} bytes from {@code bytes} to the response, starting with the given {@code offset}.
     *
     * @param bytes bytes to be written out.
     * @return The same {@link Writer} instance for writing more data.
     * @throws IOException When there is an error while writing.
     * @throws IndexOutOfBoundsException if {@code off} or {@code len} is negative or if {@code off + len} is greater
     *                                   than {@code bytes.length}.
     */
    Writer write(byte[] bytes, int off, int len) throws IOException;

    /**
     * Writes the given {@link String} to the response, using {@code UTF-8} {@link java.nio.charset.Charset Charset}.
     *
     * @param content {@link String} content to be written out.
     * @return The same {@link Writer} instance for writing more data.
     * @throws IOException When there is an error while writing.
     */
    Writer write(String content) throws IOException;
  }

  /**
   * Response code to indicate result of the {@link Procedure}.
   *
   * @deprecated As of version 2.6.0, with no direct replacement, see {@link co.cask.cdap.api.service.Service}
   */
  @Deprecated
  public enum Code {
    SUCCESS,
    FAILURE,
    CLIENT_ERROR,
    NOT_FOUND
  }

  private final Code code;

  /**
   * Construct a {@link ProcedureResponse} with the given result {@link Code}.
   *
   * @param code Result code.
   * @deprecated As of version 2.6.0, with no direct replacement, see {@link co.cask.cdap.api.service.Service}
   */
  @Deprecated
  public ProcedureResponse(Code code) {
    Preconditions.checkNotNull(code, "Response code cannot be null.");
    this.code = code;
  }
  
  /**
   *
   * @deprecated As of version 2.6.0, with no direct replacement, see {@link co.cask.cdap.api.service.Service}
   */
  @Deprecated
  public Code getCode() {
    return code;
  }
}
