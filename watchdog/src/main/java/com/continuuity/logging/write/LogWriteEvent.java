package com.continuuity.logging.write;

import com.continuuity.common.logging.LoggingContext;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.google.common.primitives.Longs;
import org.apache.avro.generic.GenericRecord;

/**
 * Represents a log event that can be written to avro file.
 */
public class LogWriteEvent implements Comparable<LogWriteEvent> {

  protected final GenericRecord  genericRecord;
  protected final ILoggingEvent logEvent;
  protected final LoggingContext loggingContext;

  public LogWriteEvent(GenericRecord genericRecord, ILoggingEvent logEvent, LoggingContext loggingContext) {
    this.genericRecord = genericRecord;
    this.logEvent = logEvent;
    this.loggingContext = loggingContext;
  }

  public GenericRecord getGenericRecord() {
    return genericRecord;
  }

  public ILoggingEvent getLogEvent() {
    return logEvent;
  }

  public LoggingContext getLoggingContext() {
    return loggingContext;
  }

  @Override
  public int compareTo(LogWriteEvent event) {
    return Longs.compare(logEvent.getTimeStamp(), event.getLogEvent().getTimeStamp());
  }
}
