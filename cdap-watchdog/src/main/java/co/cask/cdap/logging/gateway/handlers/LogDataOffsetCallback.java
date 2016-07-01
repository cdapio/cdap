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

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import co.cask.cdap.logging.read.LogEvent;
import co.cask.http.HttpResponder;

/**
 * LogReader callback to encode log events, as {@link LogData} objects.
 */
public class LogDataOffsetCallback extends AbstractJSONCallback {

  LogDataOffsetCallback(HttpResponder responder) {
    super(responder);
  }

  @Override
  public Object encodeSend(LogEvent logEvent) {
    ILoggingEvent event = logEvent.getLoggingEvent();
    StackTraceElement[] stackTraceElements = event.getCallerData();
    String className = "";
    String simpleClassName = "";
    int lineNumber = 0;
    if (stackTraceElements.length > 0) {
      StackTraceElement first = stackTraceElements[0];
      className = first.getClassName();
      if (className.lastIndexOf('.') >= 0) {
        simpleClassName = className.substring(className.lastIndexOf('.') + 1);
      }
      lineNumber = first.getLineNumber();
    }
    LogData logData = new LogData(event.getTimeStamp(), event.getLevel().toString(), event.getThreadName(),
                                  className, simpleClassName, lineNumber, event.getFormattedMessage(),
                                  ThrowableProxyUtil.asString(event.getThrowableProxy()));
    return new FormattedLogDataEvent(logData, logEvent.getOffset());
  }
}
