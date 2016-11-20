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
package co.cask.cdap.logging.serialize;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import co.cask.cdap.api.log.ClassPackagingData;
import co.cask.cdap.api.log.LoggerContextVO;
import co.cask.cdap.api.log.LoggingEvent;
import co.cask.cdap.api.log.StackTraceElementProxy;
import co.cask.cdap.api.log.ThrowableProxy;
import org.slf4j.Marker;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of {co.cask.cdap.api.log.LoggingEvent}
 */
public class LoggingEventImpl implements co.cask.cdap.api.log.LoggingEvent {
  private String threadName;
  private LoggingEvent.Level level;
  private String message;
  @Nullable
  private String[] argumentArray;
  @Nullable
  private String formattedMessage;
  @Nullable
  private String loggerName;
  @Nullable
  private LoggerContextVO loggerContextVO;
  @Nullable
  private ThrowableProxy throwableProxy;
  @Nullable
  private StackTraceElement[] callerData;
  private boolean hasCallerData;
  @Nullable
  private Marker marker;
  @Nullable
  private Map<String, String> mdc;
  private long timestamp;

  private LoggingEventImpl(ILoggingEvent iLoggingEvent) {
    this.threadName = iLoggingEvent.getThreadName();
    this.level = LoggingEvent.Level.valueOf(iLoggingEvent.getLevel().toString());
    this.message = iLoggingEvent.getMessage();
    if (iLoggingEvent.getArgumentArray() != null) {
      this.argumentArray = new String[iLoggingEvent.getArgumentArray().length];
      int i = 0;
      for (Object obj : iLoggingEvent.getArgumentArray()) {
        this.argumentArray[i++] = obj == null ? null : obj.toString();
      }
    }

    this.formattedMessage = iLoggingEvent.getFormattedMessage();
    this.loggerName = iLoggingEvent.getLoggerName();
    ch.qos.logback.classic.spi.LoggerContextVO iLoggingEventLoggerContextVO = iLoggingEvent.getLoggerContextVO();
    if (iLoggingEventLoggerContextVO != null) {
      this.loggerContextVO = new LoggerContextVO(iLoggingEventLoggerContextVO.getName(),
                                                 iLoggingEventLoggerContextVO.getPropertyMap(),
                                                 iLoggingEventLoggerContextVO.getBirthTime());
    } else {
      this.loggerContextVO = null;
    }

    if (iLoggingEvent.getThrowableProxy() != null) {
      throwableProxy = new ThrowableProxyImpl(iLoggingEvent.getThrowableProxy());
    } else {
      throwableProxy = null;
    }

    callerData = iLoggingEvent.getCallerData();
    hasCallerData = iLoggingEvent.hasCallerData();
    marker = iLoggingEvent.getMarker();
    mdc = iLoggingEvent.getMDCPropertyMap();
    timestamp = iLoggingEvent.getTimeStamp();
  }

  /**
   * Get {@link LoggingEvent} equivalent of {@link ch.qos.logback.classic.spi.LoggingEvent}
   * @param iLoggingEvent logback logging event {@link ch.qos.logback.classic.spi.LoggingEvent}
   * @return watch-dog api logging event {@link co.cask.cdap.api.log.LoggingEvent}
   */
  public static LoggingEvent getLoggingEvent(ILoggingEvent iLoggingEvent) {
    return new LoggingEventImpl(iLoggingEvent);
  }

  private class ThrowableProxyImpl implements ThrowableProxy {
    private final ch.qos.logback.classic.spi.IThrowableProxy iThrowableProxy;

    private ThrowableProxyImpl(ch.qos.logback.classic.spi.IThrowableProxy throwableProxy) {
      this.iThrowableProxy = throwableProxy;
    }
    @Override
    public String getMessage() {
      return iThrowableProxy.getMessage();
    }

    @Override
    public String getClassName() {
      return iThrowableProxy.getClassName();
    }

    @Override
    public StackTraceElementProxy[] getStackTraceElementProxyArray() {
      if (iThrowableProxy.getStackTraceElementProxyArray() != null) {
        int arrayLength = iThrowableProxy.getStackTraceElementProxyArray().length;
        StackTraceElementProxyImpl[] stackTraceElementProxyArray =
          new StackTraceElementProxyImpl[arrayLength];
        ch.qos.logback.classic.spi.StackTraceElementProxy logbackStackTraceElementProxyArray[] =
          iThrowableProxy.getStackTraceElementProxyArray();
        for (int i = 0; i < arrayLength; i++) {
          stackTraceElementProxyArray[i] = new StackTraceElementProxyImpl(logbackStackTraceElementProxyArray[i]);
        }
        return stackTraceElementProxyArray;
      }

      return null;
    }

    @Override
    public int getCommonFrames() {
      return iThrowableProxy.getCommonFrames();
    }

    @Override
    public ThrowableProxy getCause() {
      return iThrowableProxy.getCause() == null ? null : new ThrowableProxyImpl(iThrowableProxy.getCause());
    }

    @Override
    public ThrowableProxy[] getSuppressed() {
      int length = iThrowableProxy.getSuppressed().length;
      ThrowableProxy[] suppressed = new ThrowableProxy[length];
      IThrowableProxy[] iThrowableProxieSuppressed = iThrowableProxy.getSuppressed();
      for (int i = 0; i < length; i++) {
        suppressed[i] = new ThrowableProxyImpl(iThrowableProxieSuppressed[i]);
      }
      return suppressed;
    }
  }

  private class StackTraceElementProxyImpl implements StackTraceElementProxy {
    private final ClassPackagingData classPackagingData;
    private final StackTraceElement stackTraceElement;

    StackTraceElementProxyImpl(ch.qos.logback.classic.spi.StackTraceElementProxy logbackSte) {
      ch.qos.logback.classic.spi.ClassPackagingData logbackClpd = logbackSte.getClassPackagingData();
      if (logbackClpd != null) {
        this.classPackagingData = new ClassPackagingData(logbackClpd.getCodeLocation(),
                                                         logbackClpd.getVersion(), logbackClpd.isExact());
      } else {
        this.classPackagingData = null;
      }
      this.stackTraceElement = logbackSte.getStackTraceElement();
    }

    @Override
    public ClassPackagingData getClassPackagingData() {
      return classPackagingData;
    }

    @Override
    public StackTraceElement getStackTraceElement() {
      return stackTraceElement;
    }
  }

  @Override
  public String getThreadName() {
    return threadName;
  }

  @Override
  public Level getLevel() {
    return level;
  }

  @Override
  public String getMessage() {
    return message;
  }

  @Override
  public Object[] getArgumentArray() {
    return argumentArray;
  }

  @Override
  public String getFormattedMessage() {
    return formattedMessage;
  }

  @Override
  public String getLoggerName() {
    return loggerName;
  }

  @Override
  public ThrowableProxy getThrowableProxy() {
    return throwableProxy;
  }

  @Override
  public StackTraceElement[] getCallerData() {
    return callerData;
  }

  @Override
  public boolean hasCallerData() {
    return hasCallerData;
  }

  @Override
  public LoggerContextVO getLoggerContextVO() {
    return loggerContextVO;
  }

  @Override
  public Marker getMarker() {
    return marker;
  }

  @Override
  public Map<String, String> getMDCPropertyMap() {
    return mdc;
  }

  @Override
  public Map<String, String> getMdc() {
    return mdc;
  }

  @Override
  public long getTimeStamp() {
    return timestamp;
  }

  @Override
  public void prepareForDeferredProcessing() {
    // no-op
  }
}
