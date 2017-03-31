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

  @SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
  private final Long timestamp;
  @SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
  private final String logLevel;
  @SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
  private final String threadName;
  @SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
  private final String className;
  @SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
  private final String simpleClassName;
  @SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
  private final Integer lineNumber;
  @SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
  private final String message;
  @SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
  private final String stackTrace;
  @SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
  private final String loggerName;
  @SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
  private final Map<String, String> mdc;

  LogData(Long timestamp, String logLevel, String threadName, String className, String simpleClassName,
          Integer lineNumber, String message, String stackTrace, String loggerName, Map<String, String> mdc) {
    this.timestamp = timestamp;
    this.logLevel = logLevel;
    this.threadName = threadName;
    this.className = className;
    this.simpleClassName = simpleClassName;
    this.lineNumber = lineNumber;
    this.message = message;
    this.stackTrace = stackTrace;
    this.loggerName = loggerName;
    this.mdc = mdc;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public String getLogLevel() {
    return logLevel;
  }

  public String getThreadName() {
    return threadName;
  }

  public String getClassName() {
    return className;
  }

  public String getSimpleClassName() {
    return simpleClassName;
  }

  public Integer getLineNumber() {
    return lineNumber;
  }

  public String getMessage() {
    return message;
  }

  public String getStackTrace() {
    return stackTrace;
  }

  public String getLoggerName() {
    return loggerName;
  }

  public Map<String, String> getMDC() {
    return mdc;
  }
}
