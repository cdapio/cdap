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

package co.cask.cdap.etl.log;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.LoggerContextVO;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;
import org.slf4j.Marker;

import java.util.List;
import java.util.Map;

/**
 * Inserts the stage name as the start of the log message, then delegates to other appenders.
 * Uses {@link org.slf4j.MDC} to look up the current stage name.
 */
public class LogStageAppender extends AppenderBase<ILoggingEvent> {
  private final List<Appender<ILoggingEvent>> appenders;

  public LogStageAppender(List<Appender<ILoggingEvent>> appenders) {
    this.appenders = appenders;
  }

  @Override
  protected void append(ILoggingEvent eventObject) {
    ILoggingEvent stageEvent = new StageEvent(eventObject);
    for (Appender<ILoggingEvent> appender : appenders) {
      appender.doAppend(stageEvent);
    }
  }

  @Override
  public void stop() {
    super.stop();
    RuntimeException ex = null;
    for (Appender appender : appenders) {
      try {
        appender.stop();
      } catch (Throwable t) {
        if (ex == null) {
          ex = new RuntimeException(t);
        } else {
          ex.addSuppressed(t);
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
  }

  /**
   * Wrapper around ILoggingEvent that prefixes messages with the stage name if it exists.
   */
  private static class StageEvent implements ILoggingEvent {
    private final ILoggingEvent event;
    private final String eventMessage;
    private final String formattedMessage;

    StageEvent(ILoggingEvent event) {
      this.event = event;
      Map<String, String> mdcMap = event.getMDCPropertyMap();
      String stage = mdcMap.get(LogContext.STAGE);
      this.eventMessage = stage == null ? event.getMessage() : stage + " - " + event.getMessage();
      this.formattedMessage = stage == null ? event.getFormattedMessage() : stage + " - " + event.getFormattedMessage();
    }

    @Override
    public String getThreadName() {
      return event.getThreadName();
    }

    @Override
    public Level getLevel() {
      return event.getLevel();
    }

    @Override
    public String getMessage() {
      return eventMessage;
    }

    @Override
    public Object[] getArgumentArray() {
      return event.getArgumentArray();
    }

    @Override
    public String getFormattedMessage() {
      return formattedMessage;
    }

    @Override
    public String getLoggerName() {
      return event.getLoggerName();
    }

    @Override
    public LoggerContextVO getLoggerContextVO() {
      return event.getLoggerContextVO();
    }

    @Override
    public IThrowableProxy getThrowableProxy() {
      return event.getThrowableProxy();
    }

    @Override
    public StackTraceElement[] getCallerData() {
      return event.getCallerData();
    }

    @Override
    public boolean hasCallerData() {
      return event.hasCallerData();
    }

    @Override
    public Marker getMarker() {
      return event.getMarker();
    }

    @Override
    public Map<String, String> getMDCPropertyMap() {
      return event.getMDCPropertyMap();
    }

    @Override
    public Map<String, String> getMdc() {
      return event.getMdc();
    }

    @Override
    public long getTimeStamp() {
      return event.getTimeStamp();
    }

    @Override
    public void prepareForDeferredProcessing() {
      event.prepareForDeferredProcessing();
    }
  }
}
