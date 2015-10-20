/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.search.run;

import ch.qos.logback.classic.spi.StackTraceElementProxy;

/**
 * Represents the document in the ElasticSearch
 */
public final class LogSearchDocument {
  private final String index;
  private final String indexType;
  private final long timeStamp;
  private final String message;
  private boolean isException;
  private final String loggerName;
  private final StackTraceElementProxy[] stackTrace;
  private final String exceptionClassName;

  public LogSearchDocument(String index, String indexType, long timeStamp, String message,
                           String loggerName, String exceptionClassName, StackTraceElementProxy[] stackTrace) {
    this.index = index;
    this.indexType = indexType;
    this.timeStamp = timeStamp;
    this.message = message;
    this.isException = exceptionClassName != null;
    this.loggerName = loggerName;
    this.stackTrace = stackTrace;
    this.exceptionClassName = exceptionClassName;
  }

  public String getLoggerName() {
    return loggerName;
  }

  public String getIndex() {
    return index;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public String getMessage() {
    return message;
  }

  public String getIndexType() {
    return indexType;
  }

  public boolean isException() {
    return isException;
  }

  public String getExceptionClassName() {
    return exceptionClassName;
  }

  public StackTraceElementProxy[] getStackTrace() {
    return stackTrace;
  }
}
