package com.continuuity.data2.dataset2;

/**
 * Thrown when operation conflicts with existing {@link com.continuuity.api.dataset.module.DatasetModule}s
 * in the system.
 */
public class ModuleConflictException extends DatasetManagementException {
  public ModuleConflictException(String message) {
    super(message);
  }

  public ModuleConflictException(String message, Throwable cause) {
    super(message, cause);
  }
}
