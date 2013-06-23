/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.core.exceptions;

/**
 * RetryException: this exception is thrown by methods when there are potentially transient error in the system.
 * This exception indicates that the client recall the method throwing this exception at a later point in time
 */
public class RetryException extends Exception {
  public RetryException(String message) {
    super(message);
  }
}
