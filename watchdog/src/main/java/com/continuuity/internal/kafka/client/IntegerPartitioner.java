/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.kafka.client;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * A kafka {@link Partitioner} using integer key to compute partition id.
 */
public final class IntegerPartitioner implements Partitioner<Integer> {

  public IntegerPartitioner(VerifiableProperties properties) {
  }

  public int partition(Integer key, int numPartitions) {
    return key % numPartitions;
  }
}
