package com.continuuity.passport.core.exceptions;

/**
 * Exception raised when organization is not found in the system.
 */
public class OrganizationNotFoundException extends Exception {
  public OrganizationNotFoundException(String message) {
    super(message);
  }
}
