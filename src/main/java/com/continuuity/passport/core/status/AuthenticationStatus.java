package com.continuuity.passport.core.status;

import org.apache.commons.lang.StringUtils;

/**
 * Authentication Status type indicates the status and an optional message
 */
//TODO: Encoding account info in message - find a better way
public class AuthenticationStatus {

  public enum Type {AUTHENTICATED, AUTHENTICATION_FAILED};

  private Type type;

  private String message;

  public AuthenticationStatus(Type type) {
    this.type = type;
    this.message = StringUtils.EMPTY;
  }

  public AuthenticationStatus(Type type, String message) {
    this.type = type;
    this.message = message;
  }


  public Type getType() {
    return type;
  }

  public String getMessage() {
    return message;
  }
}
