package com.continuuity.passport.core.status;

import org.apache.commons.lang.StringUtils;

/**
 * Data management services Status
 */
public class Status {

  public enum Value {
    USER_REGISTERED_OK, USER_REGISTRATION_FAILED, USER_ALREADY_EXISTS,
    USER_DELETE_OK, USER_DELETE_FAILED,
    COMPONENT_REGISTRATION_OK, COMPONENT_REGISTRATION_FAILED,
    COMPONENT_UNREGISTERATION_OK, COMPONENT_UNREGISTRATION_FAILED,
    UPDATE_COMPONENT_OK, UPDATE_COMPONENT_FAILED,
    USER_AUTHENTICATION_FAILED
  }

  private Value value;

  private String reason;

  public Status(Value value) {
    this.reason = StringUtils.EMPTY;
    this.value = value;
  }


  public Status(Value value, String reason) {
    this.reason = reason;
    this.value = value;
  }

  public Value getValue() {
    return value;
  }

  public String getReason() {
    return reason;
  }
}
