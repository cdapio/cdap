package com.continuuity.passport.core.status;

import org.apache.commons.lang.StringUtils;

/**
 * Authentication Status type indicates the status and an optional message
 */
public class AuthenticationStatus {

  public enum Value {AUTHENTICATED, AUTHENTICATION_FAILED}

  ;

  private Value value;

  private String reason;

  public AuthenticationStatus(Value value) {
    this.value = value;
    this.reason = StringUtils.EMPTY;
  }

  public AuthenticationStatus(Value value, String reason) {
    this.value = value;
    this.reason = reason;
  }


  public Value getValue() {
    return value;
  }

  public String getReason() {
    return reason;
  }
}
