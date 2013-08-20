/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.appender.kafka;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.logging.appender.LogAppender;
import com.google.common.base.Throwables;
import com.google.inject.Inject;

import java.io.IOException;

/**
 * Log appender that publishes log messages to Kafka.
 */
public final class KafkaLogAppender extends LogAppender {
  public static final String APPENDER_NAME = "KafkaLogAppender";
  private final SimpleKafkaProducer producer;
  private final LoggingEventSerializer loggingEventSerializer;

  @Inject
  public KafkaLogAppender(CConfiguration configuration) {
    setName(APPENDER_NAME);
    addInfo("Initializing KafkaLogAppender...");

    this.producer = new SimpleKafkaProducer(configuration);
    try {
      this.loggingEventSerializer = new LoggingEventSerializer();
    } catch (IOException e) {
      addError("Error initializing KafkaLogAppender.", e);
      throw Throwables.propagate(e);
    }
    addInfo("Successfully initialized KafkaLogAppender.");
  }

  @Override
  protected void append(ILoggingEvent eventObject) {
    LoggingContext loggingContext = LoggingContextAccessor.getLoggingContext();
    eventObject.prepareForDeferredProcessing();
    byte [] bytes = loggingEventSerializer.toBytes(eventObject);
    producer.publish(loggingContext.getLogPartition(), bytes);
  }

  @Override
  public void stop() {
    producer.stop();
    super.stop();
  }
}
