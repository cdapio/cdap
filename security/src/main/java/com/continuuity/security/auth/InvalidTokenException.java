package com.continuuity.security.auth;

/**
 * This exception indicates a failure to validate an issued {@link AccessToken}, for example due to token expiration
 * or an invalid token digest.
 */
public class InvalidTokenException extends Exception {

  private final TokenState reason;

  public InvalidTokenException(TokenState reason, String message) {
    super(message);
    this.reason = reason;
  }

  public InvalidTokenException(TokenState reason, String message, Throwable cause) {
    super(message, cause);
    this.reason = reason;
  }

  public TokenState getReason() {
    return reason;
  }
}
