/**
 * Copyright (c) 2012 to Continuuity Inc. All rights reserved.
 * Licensed to Odiago, Inc.
 */
package com.continuuity.common.options;

/**
 * Raised when the option is present on command line, but not
 * declared.
 */
public class UnrecognizedOptionException extends RuntimeException {
  public UnrecognizedOptionException(String name) {
    super("Option " + name + " was never declared");
  }
}
