/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.kafka;

import java.nio.ByteBuffer;

/**
 * Callback interface to receive Kafka messages.
 */
public interface Callback {
  /**
   * Method that processes a Kafka message.
   * @param offset Kafka message offset of the message.
   * @param msgBuffer contains the message bytes.
   */
  void handle(long offset, ByteBuffer msgBuffer);
}
