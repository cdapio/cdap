/**
 * Copyright (c) 2012 to Continuuity Inc. All rights reserved.
 * Licensed to Odiago, Inc.
 */
package com.continuuity.common.options;

/**
 * Raised when the option specified by the type it is associated with is
 * not supported.
 */
public class UnsupportedOptionTypeException extends RuntimeException {
  public UnsupportedOptionTypeException(String name) {
    super("The @Option annotation does not support the type of field " + name);
  }
}
