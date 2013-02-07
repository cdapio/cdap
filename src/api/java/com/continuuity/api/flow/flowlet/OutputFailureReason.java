/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow.flowlet;

/**
 *
 */
public final class OutputFailureReason {

  public enum Type {
    UNKNOWN,
    IO_ERROR,
    FIELD_ACCESS_ERROR
  }

  private final Type type;
  private final String message;

  public OutputFailureReason(Type type, String message) {
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
