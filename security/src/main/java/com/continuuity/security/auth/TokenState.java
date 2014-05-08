package com.continuuity.security.auth;

/**
 * Different states attained after validating the token
 * <ul>
 *   <li>MISSING - the access token is missing in the request</li>
 *   <li>INVALID - the token digest did not match the expected value</li>
 *   <li>EXPIRED - the token is past the expiration timestamp</li>
 *   <li>INTERNAL - another error occurred in processing (represented by the exception "cause")</li>
 *   <li>VALID - the token is valid</li>
 * </ul>
 */
public enum TokenState {
  MISSING("Token is missing.", false),
  INVALID("Invalid token signature.", false),
  EXPIRED("Expired token.", false),
  INTERNAL("Invalid key for token.", false),
  VALID("Token is valid.", true);

  private final String msg;
  private final boolean valid;

  TokenState(String msg, boolean valid) {
    this.msg = msg;
    this.valid = valid;
  }

  /**
   *
   * @return the message associated with this token state describing the cause to be in this state
   */
  public String getMsg() {
    return msg;
  }

  /**
   *
   * @return {@code true} if this token state is valid, {@code false} otherwise
   */
  public boolean isValid() {
    return valid;
  }

  @Override
  public String toString() {
    return this.msg;
  }
}
