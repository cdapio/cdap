/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.appender.kafka;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.status.InfoStatus;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.logging.appender.LogAppender;
import com.google.inject.Inject;

/**
 * Log appender that publishes log messages to Kafka.
 */
public final class KafkaLogAppender extends LogAppender {
  public static final String APPENDER_NAME = "KafkaLogAppender";
  private final SimpleKafkaProducer producer;

  @Inject
  public KafkaLogAppender(CConfiguration configuration) {
    this.producer = new SimpleKafkaProducer(configuration);
    setName(APPENDER_NAME);
    addStatus(new InfoStatus("Initializing KafkaLogAppender", this));
  }

  @Override
  protected void append(ILoggingEvent eventObject) {
    LoggingContext loggingContext = LoggingContextAccessor.getLoggingContext();
    eventObject.prepareForDeferredProcessing();
    producer.publish(loggingContext.getLogPartition(), eventObject);
  }

  @Override
  public void stop() {
    producer.stop();
    super.stop();
  }
}
