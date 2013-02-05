package com.continuuity.api.procedure;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 */
public class ProcedureResponse {

  public interface Writer extends Closeable {
    Writer write(ByteBuffer buffer) throws IOException;
  }

  public enum Code {
    SUCCESS,
    FAILURE
  }

  private final Code code;
  private final String message;

  public ProcedureResponse(Code code) {
    this(code, String.format("Response %s", code.toString()));
  }

  public ProcedureResponse(Code code, String message) {
    this.code = code;
    this.message = message;
  }

  public Code getCode() {
    return code;
  }

  public String getMessage() {
    return message;
  }
}
