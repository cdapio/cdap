/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.kafka.client;

import com.continuuity.kafka.client.FetchedMessage;
import com.continuuity.kafka.client.TopicPartition;

import java.nio.ByteBuffer;

/**
 * An implementation of FetchedMessage that provides setters as well.
 */
final class BasicFetchedMessage implements FetchedMessage {

  private final TopicPartition topicPartition;
  private ByteBuffer payload;
  private long nextOffset;

  BasicFetchedMessage(TopicPartition topicPartition) {
    this.topicPartition = topicPartition;
  }

  void setPayload(ByteBuffer payload) {
    this.payload = payload;
  }

  void setNextOffset(long nextOffset) {
    this.nextOffset = nextOffset;
  }

  @Override
  public TopicPartition getTopicPartition() {
    return topicPartition;
  }

  @Override
  public ByteBuffer getPayload() {
    return payload;
  }

  @Override
  public long getNextOffset() {
    return nextOffset;
  }
}
