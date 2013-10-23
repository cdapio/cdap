package com.continuuity.api.data.dataset;

/**
 * Thrown when a data operation fails.
 */
public class DataSetException extends RuntimeException {
  public DataSetException(String message) {
    super(message);
  }

  public DataSetException(String message, Throwable cause) {
    super(message, cause);
  }
}
