/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.LoggerContextVO;
import co.cask.cdap.logging.LoggingUtil;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Marker;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import javax.annotation.Nullable;

/**
* Class used to serialize/de-serialize ILoggingEvent.
*/
public final class LoggingEvent implements ILoggingEvent {

  private final ByteBuffer encoded;
  private final GenericRecord record;

  private boolean threadNamePreserved;
  private String threadName;

  private boolean levelPreserved;
  private Level level;

  private boolean messagePreserved;
  private String message;

  private boolean argumentArrayPreserved;
  private String[] argumentArray;

  private boolean formattedMessagePreserved;
  private String formattedMessage;

  private boolean loggerNamePreserved;
  private String loggerName;

  private boolean loggerContextVOPreserved;
  private LoggerContextVO loggerContextVO;

  private boolean throwableProxyPreserved;
  private IThrowableProxy throwableProxy;

  private boolean callerDataPreserved;
  private StackTraceElement[] callerData;

  private boolean hasCallerDataPreserved;
  private boolean hasCallerData;

  private boolean mdcPreserved;
  private Map<String, String> mdc;

  private boolean timestampPreserved;
  private long timestamp;

  private boolean deferredProcessingPrepared;

  public LoggingEvent(GenericRecord record) {
    this(record, null);
  }

  public LoggingEvent(GenericRecord record, @Nullable ByteBuffer encoded) {
    this.record = record;
    this.encoded = encoded;
  }

  /**
   * Returns the {@link ByteBuffer} that this event is decoded from or {@code null} if
   * the original encoded buffer is unknown.
   */
  @Nullable
  public ByteBuffer getEncoded() {
    return encoded;
  }

  /**
   * Returns the {@link GenericRecord} that this event is constructed from.
   */
  public GenericRecord getRecord() {
    return record;
  }

  @Override
  public String getThreadName() {
    if (!threadNamePreserved) {
      threadName = LoggingUtil.stringOrNull(record.get("threadName"));
      threadNamePreserved = true;
    }
    return threadName;
  }

  @Override
  public Level getLevel() {
    if (!levelPreserved) {
      level = Level.toLevel((Integer) record.get("level"));
      levelPreserved = true;
    }
    return level;
  }

  @Override
  public String getMessage() {
    if (!messagePreserved) {
      message = LoggingUtil.stringOrNull(record.get("message"));
      messagePreserved = true;
    }
    return message;
  }

  @Override
  public Object[] getArgumentArray() {
    if (!argumentArrayPreserved) {
      GenericArray<?> argArray = (GenericArray<?>) record.get("argumentArray");
      if (argArray != null) {
        argumentArray = new String[argArray.size()];
        int i = 0;
        for (Object obj : argArray) {
          argumentArray[i++] = obj == null ? null : obj.toString();
        }
      }
      argumentArrayPreserved = true;
    }
    return argumentArray;
  }

  @Override
  public String getFormattedMessage() {
    if (!formattedMessagePreserved) {
      formattedMessage = LoggingUtil.stringOrNull(record.get("formattedMessage"));
      formattedMessagePreserved = true;
    }
    return formattedMessage;
  }

  @Override
  public String getLoggerName() {
    if (!loggerNamePreserved) {
      loggerName = LoggingUtil.stringOrNull(record.get("loggerName"));
      loggerNamePreserved = true;
    }
    return loggerName;
  }

  @Override
  public LoggerContextVO getLoggerContextVO() {
    if (!loggerContextVOPreserved) {
      loggerContextVO =  LoggerContextSerializer.decode((GenericRecord) record.get("loggerContextVO"));
      loggerContextVOPreserved = true;
    }
    return loggerContextVO;
  }

  @Override
  public IThrowableProxy getThrowableProxy() {
    if (!throwableProxyPreserved) {
      throwableProxy = ThrowableProxySerializer.decode((GenericRecord) record.get("throwableProxy"));
      throwableProxyPreserved = true;
    }
    return throwableProxy;
  }

  @Override
  public StackTraceElement[] getCallerData() {
    if (!callerDataPreserved) {
      //noinspection unchecked
      callerData = CallerDataSerializer.decode((GenericArray<GenericRecord>) record.get("callerData"));
      callerDataPreserved = true;
    }
    return callerData;
  }

  @Override
  public boolean hasCallerData() {
    if (!hasCallerDataPreserved) {
      hasCallerData = (Boolean) record.get("hasCallerData");
      hasCallerDataPreserved = true;
    }
    return hasCallerData;
  }

  @Override
  public Marker getMarker() {
    // We don't support marker in serialization, hence no need to deserializer
    return null;
  }

  @Override
  public Map<String, String> getMDCPropertyMap() {
    if (!mdcPreserved) {
      mdc = LoggingUtil.decodeMDC((Map<?, ?>) record.get("mdc"));
      mdcPreserved = true;
    }
    return mdc;
  }

  @Override
  public Map<String, String> getMdc() {
    return getMDCPropertyMap();
  }

  @Override
  public long getTimeStamp() {
    if (!timestampPreserved) {
      timestamp = (Long) record.get("timestamp");
      timestampPreserved = true;
    }
    return timestamp;
  }

  @Override
  public void prepareForDeferredProcessing() {
    if (deferredProcessingPrepared) {
      return;
    }

    // Call all the method to capture values
    getThreadName();
    getLevel();
    getMessage();
    getArgumentArray();
    getFormattedMessage();
    getLoggerName();
    getLoggerContextVO();
    getThrowableProxy();
    getCallerData();
    hasCallerData();
    getMDCPropertyMap();
    getTimeStamp();

    deferredProcessingPrepared = true;
  }

  @Override
  public String toString() {
    return "LoggingEvent{" +
      "timestamp=" + getTimeStamp() +
      ", formattedMessage='" + getFormattedMessage() + '\'' +
      ", threadName='" + getThreadName() + '\'' +
      ", level=" + getLevel() +
      ", message='" + getMessage() + '\'' +
      ", argumentArray=" + (getArgumentArray() == null ? null : Arrays.toString(getArgumentArray())) +
      ", formattedMessage='" + getFormattedMessage() + '\'' +
      ", loggerName='" + getLoggerName() + '\'' +
      ", loggerContextVO=" + getLoggerContextVO() +
      ", throwableProxy=" + getThrowableProxy() +
      ", callerData=" + (getCallerData() == null ? null : Arrays.toString(getCallerData())) +
      ", hasCallerData=" + hasCallerData() +
      ", marker=" + getMarker() +
      ", mdc=" + getMDCPropertyMap() +
      '}';
  }
}
