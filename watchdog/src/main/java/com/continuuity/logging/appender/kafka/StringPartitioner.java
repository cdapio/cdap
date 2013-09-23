/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.appender.kafka;

import com.continuuity.logging.LoggingConfiguration;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * A simple partitioner based on String keys.
 */
@SuppressWarnings("UnusedDeclaration")
public final class StringPartitioner implements Partitioner<String> {
  private final int numPartitions;

  public StringPartitioner(VerifiableProperties props) {
    this.numPartitions = Integer.parseInt(props.getProperty(LoggingConfiguration.NUM_PARTITIONS));
    Preconditions.checkArgument(this.numPartitions > 0,
                                "numPartitions should be at least 1. Got %s", this.numPartitions);
  }

  public StringPartitioner(int numPartitions) {
    this.numPartitions = numPartitions;
  }

  @Override
  public int partition(String key, int numPartitions) {
    return Math.abs(Hashing.md5().hashString(key).asInt()) % this.numPartitions;
  }
}
