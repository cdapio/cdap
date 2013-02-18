/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.error;

/**
 *
 */
public final class Errors {
  private final String format;

  public Errors(String format) {
    this.format = format;
  }

  public String getMessage(Object... objs) {
    return String.format(format, objs);
  }
}
