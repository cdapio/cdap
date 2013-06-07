package com.continuuity.common.logging.logback.serialize;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.LoggerContextVO;
import ch.qos.logback.classic.spi.ThrowableProxyVO;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.Nullable;
import org.slf4j.Marker;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
* Class used to serialize/de-serialize ILoggingEvent.
*/
public class LoggingEvent implements ILoggingEvent {
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
  private ThrowableProxyVO throwableProxy;
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
    this.level = loggingEvent.getLevel().toInt();
    this.message = loggingEvent.getMessage();

    if (loggingEvent.getArgumentArray() != null) {
      this.argumentArray = new String[loggingEvent.getArgumentArray().length];
      int i = 0;
      for (Object obj : loggingEvent.getArgumentArray()) {
        this.argumentArray[i++] = obj.toString();
      }
    }

    this.formattedMessage = loggingEvent.getFormattedMessage();
    this.loggerName = loggingEvent.getLoggerName();
    this.loggerContextVO = loggingEvent.getLoggerContextVO();
    this.throwableProxy = ThrowableProxyVO.build(loggingEvent.getThrowableProxy());

    if(loggingEvent.hasCallerData()) {
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
    datum.put("loggerContextVO", LoggerContext.encode(schema.getField("loggerContextVO").schema(),
                                                      event.getLoggerContextVO()));
    //datum.put("throwableProxy", throwableProxy);
    if (event.hasCallerData()) {
      //datum.put("callerData", callerData);
    }
    datum.put("hasCallerData", loggingEvent.hasCallerData);
    //datum.put("marker", marker);
    datum.put("mdc", event.getMDCPropertyMap());
    datum.put("timestamp", loggingEvent.timestamp);
    return datum;
  }

  public static ILoggingEvent decode(GenericRecord datum) {
    LoggingEvent loggingEvent = new LoggingEvent();
    loggingEvent.threadName = stringOrNull(datum.get("threadName"));
    loggingEvent.level = (Integer) datum.get("level");
    loggingEvent.message = stringOrNull(datum.get("message"));

    GenericArray<?> argArray = (GenericArray<?>) datum.get("argumentArray");
    if (argArray != null) {
      loggingEvent.argumentArray = new String[argArray.size()];
      for (int i = 0; i < argArray.size(); ++i) {
        loggingEvent.argumentArray[i] = argArray.get(i).toString();
      }
    }
    loggingEvent.formattedMessage = stringOrNull(datum.get("formattedMessage"));
    loggingEvent.loggerName = stringOrNull(datum.get("loggerName"));
    loggingEvent.loggerContextVO = LoggerContext.decode((GenericRecord) datum.get("loggerContextVO"));
    //loggingEvent.throwableProxy =
    //loggingEvent.callerData =
    loggingEvent.hasCallerData = (Boolean) datum.get("hasCallerData");
    //loggingEvent.marker =
    //noinspection unchecked
    loggingEvent.mdc = (Map<String, String>) datum.get("mdc");
    loggingEvent.timestamp = (Long) datum.get("timestamp");
    return loggingEvent;
  }

  private static String stringOrNull(Object obj) {
    return obj == null ? null : obj.toString();
  }

  @Override
  public String toString() {
    return "LoggingEvent{" +
      "threadName='" + threadName + '\'' +
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
      ", timestamp=" + timestamp +
      '}';
  }
}
