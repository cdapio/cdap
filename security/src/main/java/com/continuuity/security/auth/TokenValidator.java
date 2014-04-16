package com.continuuity.security.auth;

/**
 * Interface TokenValidator to validate the access token.
 */
public interface TokenValidator {
  /**
   * Different states attained after validating the token
   */
  enum State { TOKEN_MISSING, TOKEN_INVALID, TOKEN_VALID, TOKEN_UNAUTHORIZED; }

  /**
   *
   * @param token The token to be validated.
   * @return The state after validation.
   */
  State validate(String token);

  /**
   *
   * @return The error message set after validation
   */
  String getErrorMessage();
}
