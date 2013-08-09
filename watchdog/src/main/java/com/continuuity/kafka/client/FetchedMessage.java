/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.kafka.client;

import java.nio.ByteBuffer;

/**
 *
 */
public interface FetchedMessage {

  TopicPartition getTopicPartition();

  ByteBuffer getPayload();

  /**
   * Returns the offset for the next message to be read.
   */
  long getNextOffset();
}
