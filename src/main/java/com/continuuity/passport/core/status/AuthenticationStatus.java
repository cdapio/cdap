package com.continuuity.passport.core.status;

import org.apache.commons.lang.StringUtils;

/**
 * Authentication Status type indicates the status and an optional message
 */
public class AuthenticationStatus {

  public enum Type {AUTHENTICATED, AUTHENTICATION_FAILED}

  ;

  private Type type;

  private String reason;

  public AuthenticationStatus(Type type) {
    this.type = type;
    this.reason = StringUtils.EMPTY;
  }

  public AuthenticationStatus(Type type, String reason) {
    this.type = type;
    this.reason = reason;
  }


  public Type getType() {
    return type;
  }

  public String getReason() {
    return reason;
  }
}
