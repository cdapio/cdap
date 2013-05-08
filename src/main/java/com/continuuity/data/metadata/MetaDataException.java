package com.continuuity.data.metadata;

/**
 * This exception is thrown by meta data operations.
 */
public class MetaDataException extends Exception {

  public MetaDataException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public MetaDataException(String message) {
    super(message);
  }
}
