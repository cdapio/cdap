/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.serialize;

import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.logging.LoggingContextAccessor;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.LoggerContextVO;
import ch.qos.logback.classic.spi.ThrowableProxyVO;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.Nullable;
import org.slf4j.Marker;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static com.continuuity.common.logging.LoggingContext.SystemTag;
import static com.continuuity.logging.serialize.Util.stringOrNull;

/**
* Class used to serialize/de-serialize ILoggingEvent.
*/
public final class LoggingEvent implements ILoggingEvent {
  private static final int MAX_MDC_TAGS = 12;
  private static final String MDC_NULL_KEY = ".null";

  private String threadName;
  private int level;
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
  private IThrowableProxy throwableProxy;
  @Nullable
  private StackTraceElement[] callerData;
  private boolean hasCallerData;
  @Nullable
  private Marker marker;
  @Nullable
  private Map<String, String> mdc;
  private long timestamp;

  private LoggingEvent() {}

  public LoggingEvent(ILoggingEvent loggingEvent) {
    this.threadName = loggingEvent.getThreadName();
    this.level = loggingEvent.getLevel() == null ? Level.ERROR_INT : loggingEvent.getLevel().toInt();
    this.message = loggingEvent.getMessage();

    if (loggingEvent.getArgumentArray() != null) {
      this.argumentArray = new String[loggingEvent.getArgumentArray().length];
      int i = 0;
      for (Object obj : loggingEvent.getArgumentArray()) {
        this.argumentArray[i++] = obj == null ? null : obj.toString();
      }
    }

    this.formattedMessage = loggingEvent.getFormattedMessage();
    this.loggerName = loggingEvent.getLoggerName();
    this.loggerContextVO = loggingEvent.getLoggerContextVO();
    this.throwableProxy = ThrowableProxyVO.build(loggingEvent.getThrowableProxy());

    if (loggingEvent.hasCallerData()) {
      this.callerData = loggingEvent.getCallerData();
    }
    this.hasCallerData = loggingEvent.hasCallerData();

    this.marker = loggingEvent.getMarker();
    this.mdc = loggingEvent.getMDCPropertyMap();
    this.timestamp = loggingEvent.getTimeStamp();
  }

  @Override
  public String getThreadName() {
    return threadName;
  }

  @Override
  public Level getLevel() {
    return Level.toLevel(level);
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
  public LoggerContextVO getLoggerContextVO() {
    return loggerContextVO;
  }

  @Override
  public IThrowableProxy getThrowableProxy() {
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
    // Nothing to do!
  }

  public static GenericRecord encode(Schema schema, ILoggingEvent event) {
    event.prepareForDeferredProcessing();

    LoggingEvent loggingEvent = new LoggingEvent(event);
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("threadName", loggingEvent.threadName);
    datum.put("level", loggingEvent.level);
    datum.put("message", loggingEvent.message);

    if (loggingEvent.argumentArray != null) {
    GenericArray<String> argArray =
      new GenericData.Array<String>(loggingEvent.argumentArray.length,
                                    schema.getField("argumentArray").schema().getTypes().get(1));
      Collections.addAll(argArray, loggingEvent.argumentArray);
      datum.put("argumentArray", argArray);
    }

    datum.put("formattedMessage", loggingEvent.formattedMessage);
    datum.put("loggerName", loggingEvent.loggerName);
    datum.put("loggerContextVO", LoggerContextSerializer.encode(schema.getField("loggerContextVO").schema(),
                                                                loggingEvent.loggerContextVO));
    datum.put("throwableProxy", ThrowableProxySerializer.encode(schema.getField("throwableProxy").schema(),
                                                                loggingEvent.throwableProxy));
    if (loggingEvent.hasCallerData) {
      datum.put("callerData", CallerDataSerializer.encode(schema.getField("callerData").schema(),
                                                                 loggingEvent.callerData));
    }
    datum.put("hasCallerData", loggingEvent.hasCallerData);
    //datum.put("marker", marker);
    datum.put("mdc", generateContextMdc(loggingEvent.getMDCPropertyMap()));
    datum.put("timestamp", loggingEvent.timestamp);
    return datum;
  }

  static Map<String, String> generateContextMdc(Map<String, String> mdc) {
    LoggingContext loggingContext = LoggingContextAccessor.getLoggingContext();
    if (loggingContext == null) {
      throw new IllegalStateException(String.format("Logging context not setup correctly for MDC %s", mdc));
    }

    Map<String, String> contextMdcMap = encodeMdcMap(mdc);

    Map<String, SystemTag> systemTagMap = LoggingContextAccessor.getLoggingContext().getSystemTagsMap();
    for (Map.Entry<String, SystemTag> entry : systemTagMap.entrySet()) {
      contextMdcMap.put(entry.getKey(), entry.getValue().getValue());
    }
    return contextMdcMap;
  }

  static Map<String, String> encodeMdcMap(Map<String, String> mdc) {
    Map<String, String> encodeMap = Maps.newHashMapWithExpectedSize(MAX_MDC_TAGS * 2);
    int i = 0;
    for (Map.Entry<String, String> entry : mdc.entrySet()) {
      if (i++ > MAX_MDC_TAGS) {
        break;
      }
      // Any tag beginning with . is reserved
      if (entry.getKey() == null || !entry.getKey().startsWith(".")) {
        // AVRO does not allow null map keys.
        encodeMap.put(entry.getKey() == null ? MDC_NULL_KEY : entry.getKey(), entry.getValue());
      }
    }
    return encodeMap;
  }

  @SuppressWarnings("unchecked")
  public static ILoggingEvent decode(GenericRecord datum) {
    LoggingEvent loggingEvent = new LoggingEvent();
    loggingEvent.threadName = stringOrNull(datum.get("threadName"));
    loggingEvent.level = (Integer) datum.get("level");
    loggingEvent.message = stringOrNull(datum.get("message"));

    GenericArray<?> argArray = (GenericArray<?>) datum.get("argumentArray");
    if (argArray != null) {
      loggingEvent.argumentArray = new String[argArray.size()];
      for (int i = 0; i < argArray.size(); ++i) {
        loggingEvent.argumentArray[i] = argArray.get(i) == null ? null : argArray.get(i).toString();
      }
    }
    loggingEvent.formattedMessage = stringOrNull(datum.get("formattedMessage"));
    loggingEvent.loggerName = stringOrNull(datum.get("loggerName"));
    loggingEvent.loggerContextVO = LoggerContextSerializer.decode((GenericRecord) datum.get("loggerContextVO"));
    loggingEvent.throwableProxy = ThrowableProxySerializer.decode((GenericRecord) datum.get("throwableProxy"));
    loggingEvent.callerData = CallerDataSerializer.decode((GenericArray<GenericRecord>) datum.get("callerData"));
    loggingEvent.hasCallerData = (Boolean) datum.get("hasCallerData");
    loggingEvent.mdc = decodeMdcMap((Map<?, ?>) datum.get("mdc"));
    loggingEvent.timestamp = (Long) datum.get("timestamp");
    return loggingEvent;
  }

  static Map<String, String> decodeMdcMap(Map<?, ?> map) {
    if (map == null) {
      return null;
    }

    Map<String, String> stringMap = Maps.newHashMapWithExpectedSize(map.size());
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      // AVRO does not allow null map keys.
      stringMap.put(entry.getKey() == null || entry.getKey().toString().equals(MDC_NULL_KEY) ?
                      null : entry.getKey().toString(),
                    entry.getValue() == null ? null : entry.getValue().toString());
    }
    return stringMap;
  }

  @Override
  public String toString() {
    return "LoggingEvent{" +
      "timestamp=" + timestamp +
      ", formattedMessage='" + formattedMessage + '\'' +
      ", threadName='" + threadName + '\'' +
      ", level=" + Level.toLevel(level) +
      ", message='" + message + '\'' +
      ", argumentArray=" + (argumentArray == null ? null : Arrays.asList(argumentArray)) +
      ", formattedMessage='" + formattedMessage + '\'' +
      ", loggerName='" + loggerName + '\'' +
      ", loggerContextVO=" + loggerContextVO +
      ", throwableProxy=" + throwableProxy +
      ", callerData=" + (callerData == null ? null : Arrays.asList(callerData)) +
      ", hasCallerData=" + hasCallerData +
      ", marker=" + marker +
      ", mdc=" + mdc +
      '}';
  }
}
