/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.kafka.client;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 *
 */
public final class IntegerPartitioner implements Partitioner<Integer> {

  public IntegerPartitioner(VerifiableProperties properties) {
  }

  public int partition(Integer key, int numPartitions) {
    return key % numPartitions;
  }
}
