/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.kafka.client;

import com.google.common.primitives.Ints;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

/**
 * A kafka {@link Encoder} for encoding integer into bytes.
 */
public final class IntegerEncoder implements Encoder<Integer> {

  public IntegerEncoder(VerifiableProperties properties) {
  }

  public byte[] toBytes(Integer buffer) {
    return Ints.toByteArray(buffer.intValue());
  }
}
