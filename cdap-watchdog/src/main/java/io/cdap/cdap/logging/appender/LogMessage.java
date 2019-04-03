/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.logging.appender;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.LoggerContextVO;
import co.cask.cdap.common.logging.LoggingContext;
import com.google.common.base.Objects;
import org.slf4j.Marker;

import java.util.Map;

/**
 * Represents an event to be logged along with the context.
 */
public class LogMessage implements ILoggingEvent {

  private final ILoggingEvent loggingEvent;
  private final LoggingContext loggingContext;
  private final Map<String, String> mdc;
  private Object[] argumentArray;

  public LogMessage(ILoggingEvent loggingEvent, LoggingContext loggingContext) {
    this.loggingEvent = loggingEvent;
    this.loggingContext = loggingContext;
    this.mdc = new LoggingContextMDC(loggingContext.getSystemTagsAsString(), loggingEvent.getMDCPropertyMap());
  }

  public LoggingContext getLoggingContext() {
    return loggingContext;
  }

  @Override
  public String getThreadName() {
    return loggingEvent.getThreadName();
  }

  @Override
  public Level getLevel() {
    return loggingEvent.getLevel();
  }

  @Override
  public String getMessage() {
    return loggingEvent.getMessage();
  }

  @Override
  public Object[] getArgumentArray() {
    return argumentArray == null ? loggingEvent.getArgumentArray() : argumentArray;
  }

  @Override
  public String getFormattedMessage() {
    return loggingEvent.getFormattedMessage();
  }

  @Override
  public String getLoggerName() {
    return loggingEvent.getLoggerName();
  }

  @Override
  public LoggerContextVO getLoggerContextVO() {
    return loggingEvent.getLoggerContextVO();
  }

  @Override
  public IThrowableProxy getThrowableProxy() {
    return loggingEvent.getThrowableProxy();
  }

  @Override
  public StackTraceElement[] getCallerData() {
    return loggingEvent.getCallerData();
  }

  @Override
  public boolean hasCallerData() {
    return loggingEvent.hasCallerData();
  }

  @Override
  public Marker getMarker() {
    return loggingEvent.getMarker();
  }

  @Override
  public Map<String, String> getMDCPropertyMap() {
    return mdc;
  }

  @Override
  public Map<String, String> getMdc() {
    return getMDCPropertyMap();
  }

  @Override
  public long getTimeStamp() {
    return loggingEvent.getTimeStamp();
  }

  @Override
  public void prepareForDeferredProcessing() {
    loggingEvent.prepareForDeferredProcessing();
    Object[] args = loggingEvent.getArgumentArray();
    if (args != null && argumentArray == null) {
      argumentArray = new Object[args.length];
      for (int i = 0; i < args.length; i++) {
        argumentArray[i] = args[i].toString();
      }
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("loggingEvent", loggingEvent)
      .add("loggingContext", loggingContext)
      .toString();
  }
}
