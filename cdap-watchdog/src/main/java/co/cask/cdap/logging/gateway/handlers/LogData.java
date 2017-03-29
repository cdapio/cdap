/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.logging.gateway.handlers;

import java.util.Map;

/**
 * Represents the structure of a log event.
 */
public final class LogData {

  private final Long timestamp;
  private final String logLevel;
  private final String threadName;
  private final String className;
  private final String simpleClassName;
  private final Integer lineNumber;
  private final String message;
  private final String stackTrace;
  private final String loggerName;
  private final Map<String, String> mdcMap;

  LogData(Long timestamp, String logLevel, String threadName, String className, String simpleClassName,
          Integer lineNumber, String message, String stackTrace, String loggerName, Map<String, String> mdcMap) {
    this.timestamp = timestamp;
    this.logLevel = logLevel;
    this.threadName = threadName;
    this.className = className;
    this.simpleClassName = simpleClassName;
    this.lineNumber = lineNumber;
    this.message = message;
    this.stackTrace = stackTrace;
    this.loggerName = loggerName;
    this.mdcMap = mdcMap;
  }

  @SuppressWarnings("UnusedDeclaration")
  public Long getTimestamp() {
    return timestamp;
  }

  @SuppressWarnings("UnusedDeclaration")
  public String getLogLevel() {
    return logLevel;
  }

  @SuppressWarnings("UnusedDeclaration")
  public String getThreadName() {
    return threadName;
  }

  @SuppressWarnings("UnusedDeclaration")
  public String getClassName() {
    return className;
  }

  @SuppressWarnings("UnusedDeclaration")
  public String getSimpleClassName() {
    return simpleClassName;
  }

  @SuppressWarnings("UnusedDeclaration")
  public Integer getLineNumber() {
    return lineNumber;
  }

  @SuppressWarnings("UnusedDeclaration")
  public String getMessage() {
    return message;
  }

  @SuppressWarnings("UnusedDeclaration")
  public String getStackTrace() {
    return stackTrace;
  }

  @SuppressWarnings("UnusedDeclaration")
  public String getLoggerName() {
    return loggerName;
  }

  @SuppressWarnings("UnusedDeclaration")
  public Map<String, String> getMDCMap() {
    return mdcMap;
  }
}
