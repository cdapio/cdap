/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.appender.kafka;

import com.continuuity.logging.LoggingConfiguration;
import com.google.common.base.Preconditions;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.apache.hadoop.io.MD5Hash;

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

  @Override
  public int partition(String key, int numPartitions) {
    return MD5Hash.digest(key).hashCode() % this.numPartitions;
  }
}
