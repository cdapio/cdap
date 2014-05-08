package com.continuuity.security.auth;

/**
 * Interface TokenValidator to validate the access token.
 */
public interface TokenValidator {
  /**
   * Validates the access token and returns the {@link com.continuuity.security.auth.TokenState}
   * describing the casue to be in this state
   *
   * @param token The token to be validated.
   * @return The state after validation.
   */
  TokenState validate(String token);
}
