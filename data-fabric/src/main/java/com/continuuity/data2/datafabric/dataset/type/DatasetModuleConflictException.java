package com.continuuity.data2.datafabric.dataset.type;

/**
 * Thrown when operation on the dataset types cannot be performed due to conflict with
 * other existing dataset modules in the system
 */
public class DatasetModuleConflictException extends Exception {
  public DatasetModuleConflictException(String message) {
    super(message);
  }
}
