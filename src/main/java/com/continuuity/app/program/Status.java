/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.program;

/**
 * Result of running a program
 */
public enum Status {
  /**
   * Program execution ended successfully
   */
  SUCCEEDED,
  /**
   * Program execution was stopped
   */
  STOPPED,
  /**
   * Program execution failed
   */
  FAILED,
}
