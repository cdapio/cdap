package com.continuuity.logging.kafka;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.common.logging.LoggingContext;
import org.apache.avro.generic.GenericRecord;

/**
 * Represents a log event fetched from Kafka.
 */
public final class KafkaLogEvent {
  private final GenericRecord  genericRecord;
  private final ILoggingEvent logEvent;
  private final long offset;
  private final LoggingContext loggingContext;

  public KafkaLogEvent(GenericRecord genericRecord, ILoggingEvent logEvent, long offset,
                       LoggingContext loggingContext) {
    this.genericRecord = genericRecord;
    this.logEvent = logEvent;
    this.offset = offset;
    this.loggingContext = loggingContext;
  }

  public GenericRecord getGenericRecord() {
    return genericRecord;
  }

  public ILoggingEvent getLogEvent() {
    return logEvent;
  }

  public long getOffset() {
    return offset;
  }

  public LoggingContext getLoggingContext() {
    return loggingContext;
  }
}
