/**
 * Copyright (c) 2012 to Continuuity Inc. All rights reserved.
 * Licensed to Odiago, Inc.
 */

package com.continuuity.common.options;


/**
 * Thrown when there is duplicate option specified in definition.
 */
public class DuplicateOptionException extends RuntimeException {
  public DuplicateOptionException(String name) {
    super("Duplicate @Option declaration " + name);
  }
}
