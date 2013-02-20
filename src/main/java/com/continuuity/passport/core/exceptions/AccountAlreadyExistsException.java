package com.continuuity.passport.core.exceptions;

/**
 *
 */
public class AccountAlreadyExistsException extends Exception {
  public AccountAlreadyExistsException(String message) {
    super(message);
  }

  public AccountAlreadyExistsException(String message, Exception cause) {
    super(message, cause);
  }

}
