package com.continuuity.data2.dataset2;

/**
 * Thrown when operation conflicts with existing data set types in the system.
 */
public class TypeConflictException extends DatasetManagementException {
  public TypeConflictException(String message) {
    super(message);
  }

  public TypeConflictException(String message, Throwable cause) {
    super(message, cause);
  }
}
