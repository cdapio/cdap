package com.continuuity.security.auth;

/**
 * This exception indicates a failure to validate an issued {@link AccessToken}, for example due to token expiration
 * or an invalid token digest.
 */
public class InvalidTokenException extends Exception {
  /**
   * Enum representing why a token was rejected.  Possible reasons are:
   * <ul>
   *   <li>INVALID - the token digest did not match the expected value</li>
   *   <li>EXPIRED - the token is past the expiration timestamp</li>
   *   <li>INTERNAL - another error occurred in processing (represented by the exception "cause")</li>
   * </ul>
   */
  public enum Reason { INVALID, EXPIRED, INTERNAL };

  private final Reason reason;

  public InvalidTokenException(Reason reason, String message) {
    super(message);
    this.reason = reason;
  }

  public InvalidTokenException(Reason reason, String message, Throwable cause) {
    super(message, cause);
    this.reason = reason;
  }

  public Reason getReason() {
    return reason;
  }
}
