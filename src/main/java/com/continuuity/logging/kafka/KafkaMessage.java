package com.continuuity.logging.kafka;

import java.nio.ByteBuffer;

/**
 * Represents a Kafka message.
 */
public final class KafkaMessage {
  private final ByteBuffer byteBuffer;
  private final long offset;

  public KafkaMessage(ByteBuffer byteBuffer, long offset) {
    this.byteBuffer = byteBuffer;
    this.offset = offset;
  }

  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }

  public long getOffset() {
    return offset;
  }
}
