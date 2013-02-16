/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.app.program;

/**
 * Result of running a program
 */
public enum ProgramRunResult {
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
