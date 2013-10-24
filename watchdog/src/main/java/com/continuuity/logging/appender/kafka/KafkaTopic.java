/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.appender.kafka;

import java.io.IOException;

/**
 * Generates Kafka topic containing schema for logging.
 */
public final class KafkaTopic {
  // Kafka topic on which log messages will get published.
  // If there is an incompatible log schema change, then the topic version needs to be updated.
  private static final String KAFKA_TOPIC = "logs.user-v1";

  /**
   * @return Kafka topic with schema.
   * @throws IOException
   */
  public static String getTopic() throws IOException {
    return KAFKA_TOPIC;
  }
}
