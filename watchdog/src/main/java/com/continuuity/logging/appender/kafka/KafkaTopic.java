/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.appender.kafka;

import java.io.IOException;

/**
 * Generates Kafka topic containing schema for logging.
 */
public final class KafkaTopic {
  private static final String KAFKA_TOPIC = "logs.USER";

  /**
   * @return Kafka topic with schema.
   * @throws IOException
   */
  public static String getTopic() throws IOException {
    return KAFKA_TOPIC;
  }
}
