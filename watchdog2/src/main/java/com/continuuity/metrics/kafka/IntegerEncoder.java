/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.kafka;

import com.google.common.primitives.Ints;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

/**
 *
 */
public final class IntegerEncoder implements Encoder<Integer> {

  public IntegerEncoder(VerifiableProperties properties) {
  }

  public byte[] toBytes(Integer buffer) {
    return Ints.toByteArray(buffer.intValue());
  }
}