package com.continuuity.passport.core.exceptions;

/**
 * Exception raised when Organization already exists in the system and the operation that is performed in the system
 * assumes that the organization doesn't exist.
 */
public class OrganizationAlreadyExistsException extends Exception {
  public OrganizationAlreadyExistsException(String message) {
    super(message);
  }
}
