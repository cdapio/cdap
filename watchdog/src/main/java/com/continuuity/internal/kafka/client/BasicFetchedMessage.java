/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.kafka.client;

import com.continuuity.kafka.client.FetchedMessage;

import java.nio.ByteBuffer;

/**
 * An implementation of FetchedMessage that provides setters as well.
 */
final class BasicFetchedMessage implements FetchedMessage {

  private final String topic;
  private final int partition;
  private ByteBuffer payload;
  private long nextOffset;

  BasicFetchedMessage(String topic, int partition) {
    this.topic = topic;
    this.partition = partition;
  }

  void setPayload(ByteBuffer payload) {
    this.payload = payload;
  }

  void setNextOffset(long nextOffset) {
    this.nextOffset = nextOffset;
  }

  @Override
  public String getTopic() {
    return topic;
  }

  @Override
  public int getPartition() {
    return partition;
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
