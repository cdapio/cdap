/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
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
