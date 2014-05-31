package com.continuuity.data2.dataset2;

/**
 * Thrown when operation conflicts with existing dataset instances available in the system.
 */
public class InstanceConflictException extends DatasetManagementException {
  public InstanceConflictException(String message) {
    super(message);
  }

  public InstanceConflictException(String message, Throwable cause) {
    super(message, cause);
  }
}
