/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.common.logging.logback.kafka;

import com.continuuity.common.logging.logback.serialize.LogSchema;

import java.io.IOException;

/**
 * Generates Kafka topic containing schema for logging.
 */
public final class KafkaTopic {
  private static final String KAFKA_TOPIC_PREFIX = "LOG_MESSAGES";

  /**
   * @return Kafka topic with schema.
   * @throws IOException
   */
  public static String getTopic() throws IOException {
    LogSchema logSchema = new LogSchema();
    return String.format("%s_%s", KAFKA_TOPIC_PREFIX, logSchema.getSchemaHash().toString());
  }
}
