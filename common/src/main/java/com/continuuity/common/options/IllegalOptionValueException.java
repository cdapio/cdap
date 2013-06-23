/**
 * Copyright (c) 2012 to Continuuity Inc. All rights reserved.
 * Licensed to Odiago, Inc.
 */
package com.continuuity.common.options;


/**
 * Raised when there is an Illegal option value. This happens when
 * there is an @Option annotation associated with the type that is
 * not handled by <code>OptionSpec</code>
 */
public class IllegalOptionValueException extends RuntimeException {
  public IllegalOptionValueException(String name, String value) {
    super("Iiegal value for option " + name + ":" + value);
  }
}
