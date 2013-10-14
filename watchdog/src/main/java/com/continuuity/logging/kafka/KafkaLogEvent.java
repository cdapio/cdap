/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.kafka;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.common.logging.LoggingContext;
import com.google.common.primitives.Longs;
import org.apache.avro.generic.GenericRecord;

/**
 * Represents a log event fetched from Kafka.
 */
public final class KafkaLogEvent implements Comparable<KafkaLogEvent> {
  private final GenericRecord  genericRecord;
  private final ILoggingEvent logEvent;
  private final LoggingContext loggingContext;
  private final int partition;
  private final long nextOffset;

  public KafkaLogEvent(GenericRecord genericRecord, ILoggingEvent logEvent, LoggingContext loggingContext,
                       int partition, long nextOffset) {
    this.genericRecord = genericRecord;
    this.logEvent = logEvent;
    this.loggingContext = loggingContext;
    this.partition = partition;
    this.nextOffset = nextOffset;
  }

  public GenericRecord getGenericRecord() {
    return genericRecord;
  }

  public ILoggingEvent getLogEvent() {
    return logEvent;
  }

  public int getPartition() {
    return partition;
  }

  public long getNextOffset() {
    return nextOffset;
  }

  public LoggingContext getLoggingContext() {
    return loggingContext;
  }

  @Override
  public int compareTo(KafkaLogEvent event) {
    return Longs.compare(logEvent.getTimeStamp(), event.getLogEvent().getTimeStamp());
  }
}
