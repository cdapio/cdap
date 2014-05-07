package com.continuuity.security.auth;

/**
 * Interface TokenValidator to validate the access token.
 */
public interface TokenValidator {
  /**
   * Different states attained after validating the token
   */
  enum State {
    TOKEN_MISSING("Token is missing.", false),
    TOKEN_INVALID("Invalid token signature.", false),
    TOKEN_EXPIRED("Expired token.", false),
    TOKEN_INTERNAL("Invalid key for token.", false),
    TOKEN_VALID("Token is valid.", true),
    TOKEN_UNAUTHORIZED("Token is unauthorized.", false);

    private final String msg;
    private final boolean valid;

    State(String msg, boolean valid) {
      this.msg = msg;
      this.valid = valid;
    }
    public String getMsg() {
      return msg;
    }

    public boolean isValid() {
      return valid;
    }

    @Override
    public String toString() {
      return this.msg;
    }
  }

  /**
   *
   * @param token The token to be validated.
   * @return The state after validation.
   */
  State validate(String token);
}
