/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.common.logging.common;

/**
 * Defines log writer interface.
 */
public interface LogWriter {
  /**
   * Write a log message with a given log level.
   * @param tag   tag
   * @param level log level
   * @param message log message
   * @return  true on successful write
   */
  public boolean write(String tag, String level, String message);
}
