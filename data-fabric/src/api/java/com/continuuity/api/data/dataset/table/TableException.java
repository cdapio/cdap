package com.continuuity.api.data.dataset.table;

/**
 * Thrown when the execution of a table data operation fails.
 */
public class TableException extends RuntimeException {
  public TableException(String message) {
    super(message);
  }

  public TableException(String message, Throwable cause) {
    super(message, cause);
  }
}
