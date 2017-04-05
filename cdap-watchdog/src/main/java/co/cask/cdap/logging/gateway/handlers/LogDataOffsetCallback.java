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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.List;

/**
 * LogReader callback to encode log events, as {@link LogData} objects.
 */
public class LogDataOffsetCallback extends AbstractJSONCallback {
  private final List<String> fieldsToSuppress;

  LogDataOffsetCallback(HttpResponder responder, List<String> fieldsToSuppress) {
    super(responder);
    this.fieldsToSuppress = fieldsToSuppress;
    validate();
  }

  @Override
  public Object encodeSend(LogEvent logEvent) {
    ILoggingEvent event = logEvent.getLoggingEvent();
    StackTraceElement[] stackTraceElements = event.getCallerData();
    String className = "";
    String simpleClassName = "";
    int lineNumber = 0;
    if (stackTraceElements != null && stackTraceElements.length > 0) {
      StackTraceElement first = stackTraceElements[0];
      className = first.getClassName();
      simpleClassName = (className.indexOf('.') >= 0) ? className.substring(className.lastIndexOf('.') + 1) : className;
      lineNumber = first.getLineNumber();
    }
    LogData logData = new LogData(event.getTimeStamp(), event.getLevel().toString(), event.getThreadName(),
                                  className, simpleClassName, lineNumber, event.getFormattedMessage(),
                                  ThrowableProxyUtil.asString(event.getThrowableProxy()), event.getLoggerName(),
                                  event.getMDCPropertyMap());
    return modifyLogJsonElememnt(GSON.toJsonTree(new FormattedLogDataEvent(logData, logEvent.getOffset())));
  }

  private Object modifyLogJsonElememnt(JsonElement jsonElement) {
    JsonObject jsonLogData = (JsonObject) jsonElement;
    JsonObject logData = jsonLogData.getAsJsonObject("log");

    if (!fieldsToSuppress.isEmpty()) {
      for (String field : fieldsToSuppress) {
        logData.remove(field);
      }
    }

    return jsonLogData;
  }

  private void validate() {
    if (fieldsToSuppress.isEmpty()) {
      return;
    }

    for (String fieldToSuppress : fieldsToSuppress) {
      try {
        LogData.class.getDeclaredField(fieldToSuppress);
      } catch (NoSuchFieldException e) {
        throw new IllegalArgumentException(String.format("Field %s is not supported as suppress field",
                                                         fieldToSuppress));
      }
    }
  }
}
