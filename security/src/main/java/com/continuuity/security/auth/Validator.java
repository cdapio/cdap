package com.continuuity.security.auth;

/**
 * Interface Validator to validate the access token.
 */
public interface Validator {
  /**
   * Different states attained after validating the token
   */
  public enum State{ TOKEN_MISSING, TOKEN_INVALID, TOKEN_VALID, TOKEN_UNAUTHORIZED; }

  /**
   *
   * @param token The token to be validated.
   * @return The state after validation.
   */
  public State validate(String token);
}
