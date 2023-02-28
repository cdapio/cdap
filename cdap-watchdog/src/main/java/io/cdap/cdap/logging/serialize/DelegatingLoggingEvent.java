/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.logging.serialize;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.LoggerContextVO;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Marker;

public class DelegatingLoggingEvent implements ILoggingEvent {

  private final ILoggingEvent loggingEvent;
  private final Map<String, String> mdc;

  public DelegatingLoggingEvent(ILoggingEvent event, Map<String, String> mdc) {
    this.loggingEvent = event;
    this.mdc = Collections.unmodifiableMap(new HashMap<>(mdc));
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
    return loggingEvent.getArgumentArray();
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
    return this.mdc;
  }

  @Override
  public Map<String, String> getMdc() {
    return this.getMDCPropertyMap();
  }

  @Override
  public long getTimeStamp() {
    return loggingEvent.getTimeStamp();
  }

  @Override
  public void prepareForDeferredProcessing() {
    loggingEvent.prepareForDeferredProcessing();
  }
}
