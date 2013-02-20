/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.procedure;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class represents the response from a procedure.
 */
public final class ProcedureResponse {

  /**
   * Interface for writing response data.
   */
  public interface Writer extends Closeable {

    /**
     * Writes the content of the given buffer to the response.
     * @param buffer {@link ByteBuffer} holding the content to be written. After this method
     *               returns, the {@link ByteBuffer} is drained.
     * @return The same {@link Writer} instance for writing more data.
     * @throws IOException When there is error while writing.
     */
    Writer write(ByteBuffer buffer) throws IOException;
  }

  /**
   * Response code to indicate result of the {@link Procedure}.
   */
  public enum Code {
    SUCCESS,
    FAILURE
  }

  private final Code code;

  /**
   * Construct a {@link ProcedureResponse} with the given result {@link Code}.
   * @param code Result code.
   */
  public ProcedureResponse(Code code) {
    Preconditions.checkNotNull(code, "Response code cannot be null.");
    this.code = code;
  }

  public Code getCode() {
    return code;
  }
}
