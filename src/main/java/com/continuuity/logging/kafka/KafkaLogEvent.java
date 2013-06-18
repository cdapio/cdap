/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

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
  private final LoggingContext loggingContext;
  private final long offset;

  public KafkaLogEvent(GenericRecord genericRecord, ILoggingEvent logEvent, LoggingContext loggingContext,
                       long offset) {
    this.genericRecord = genericRecord;
    this.logEvent = logEvent;
    this.loggingContext = loggingContext;
    this.offset = offset;
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
