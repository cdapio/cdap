package com.continuuity.security.auth;

/**
 * Interface TokenValidator to validate the access token.
 */
public interface TokenValidator {
  /**
   *
   * @param token The token to be validated.
   * @return The state after validation.
   */
  TokenState validate(String token);
}
