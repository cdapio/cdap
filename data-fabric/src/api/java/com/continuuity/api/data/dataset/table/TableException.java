package com.continuuity.api.data.dataset.table;

/**
 * Thrown when execution of table data operation fails
 */
public class TableException extends RuntimeException {
  public TableException(String message) {
    super(message);
  }

  public TableException(String message, Throwable cause) {
    super(message, cause);
  }
}
