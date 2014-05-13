package com.continuuity.security.auth;

import com.google.common.util.concurrent.Service;

/**
 * Interface TokenValidator to validate the access token.
 */
public interface TokenValidator extends Service {
  /**
   * Validates the access token and returns the {@link com.continuuity.security.auth.TokenState}
   * describing the cause to be in this state
   *
   * @param token The token to be validated.
   * @return The state after validation.
   */
  TokenState validate(String token);
}
