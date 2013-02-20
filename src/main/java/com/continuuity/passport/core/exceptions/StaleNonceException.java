package com.continuuity.passport.core.exceptions;

/**
 *
 */
public class StaleNonceException extends Exception {
  public StaleNonceException(String message) {
    super(message);
  }

  public StaleNonceException(String message, Exception cause) {
    super(message, cause);
  }
}
