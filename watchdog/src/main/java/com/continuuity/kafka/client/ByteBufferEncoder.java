/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.kafka.client;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import java.nio.ByteBuffer;

/**
 *
 */
public final class ByteBufferEncoder implements Encoder<ByteBuffer> {

  public ByteBufferEncoder(VerifiableProperties properties) {
  }

  public byte[] toBytes(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }
}
