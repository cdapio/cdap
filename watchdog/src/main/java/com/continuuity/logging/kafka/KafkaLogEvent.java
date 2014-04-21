/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.kafka;

import com.continuuity.common.logging.LoggingContext;
import com.continuuity.logging.write.LogWriteEvent;
import ch.qos.logback.classic.spi.ILoggingEvent;
import org.apache.avro.generic.GenericRecord;

/**
 * Represents a log event fetched from Kafka.
 */
public final class KafkaLogEvent extends LogWriteEvent {
  private final int partition;
  private final long nextOffset;

  public KafkaLogEvent(GenericRecord genericRecord, ILoggingEvent logEvent, LoggingContext loggingContext,
                       int partition, long nextOffset) {
    super(genericRecord, logEvent, loggingContext);
    this.partition = partition;
    this.nextOffset = nextOffset;
  }

  public int getPartition() {
    return partition;
  }

  public long getNextOffset() {
    return nextOffset;
  }

}
