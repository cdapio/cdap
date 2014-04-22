package com.continuuity.security.auth;

/**
 * Interface TokenValidator to validate the access token.
 */
public interface TokenValidator {
  /**
   * Different states attained after validating the token
   */
  enum State {
    TOKEN_MISSING("Token is missing."),
    TOKEN_INVALID("Invalid token signature."),
    TOKEN_EXPIRED("Expired token."),
    TOKEN_INTERNAL("Invalid key for token."),
    TOKEN_VALID("Token is valid."),
    TOKEN_UNAUTHORIZED("Token is unauthorized.");

    private final String msg;

    State(String msg) {
      this.msg = msg;
    }
    public String getMsg() {
      return msg;
    }
  }

  /**
   *
   * @param token The token to be validated.
   * @return The state after validation.
   */
  State validate(String token);
}
