/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.core.status;

import org.apache.commons.lang.StringUtils;

/**
 * Defines AuthenticationStatus.
 */
public class AuthenticationStatus {

  /**
   * Authentication Type - possible values: AUTHENTICATED - if the authentication was successful, AUTHENTICATION_FAILED
   * - if the Authentication failed.
   */
  public enum Type { AUTHENTICATED, AUTHENTICATION_FAILED };

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
