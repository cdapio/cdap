/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow.flowlet;

/**
 *
 */
public class FlowletException extends Exception {

  public FlowletException(String message) {
    super(message);
  }

  public FlowletException(String message, Throwable cause) {
    super(message, cause);
  }

  public FlowletException(Throwable cause) {
    super(cause);
  }
}
