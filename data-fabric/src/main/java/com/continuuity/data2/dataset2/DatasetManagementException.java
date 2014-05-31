package com.continuuity.data2.dataset2;

/**
 * Thrown when there's an error during dataset modules, types or instances management exception
 */
public class DatasetManagementException extends Exception {
  public DatasetManagementException(String message) {
    super(message);
  }

  public DatasetManagementException(String message, Throwable cause) {
    super(message, cause);
  }
}
