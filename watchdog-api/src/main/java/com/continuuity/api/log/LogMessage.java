/*
 * com.continuuity - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */

package com.continuuity.api.log;

/**
 *
 */
public interface LogMessage {

  /**
   *
   */
  public static enum LogLevel {
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR
  }

  /**
   * @return log message timestamp
   */
  long getTimestamp();

  /**
   * @return log message text
   */
  String getText();

  /**
   * @return log level
   */
  LogLevel getLogLevel();

  /**
   * @return user markers this log message is tagged with
   */
  String[] getMarkers();
}
