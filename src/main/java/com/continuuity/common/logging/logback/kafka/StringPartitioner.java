package com.continuuity.common.logging.logback.kafka;

import com.continuuity.common.logging.LoggingConfiguration;
import com.google.common.base.Preconditions;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * A simple partitioner based on String keys.
 */
@SuppressWarnings("UnusedDeclaration")
public class StringPartitioner implements Partitioner<String> {
  private final int numPartitions;

  public StringPartitioner(VerifiableProperties props) {
    this.numPartitions = Integer.parseInt(props.getProperty(LoggingConfiguration.NUM_PARTITONS));
    Preconditions.checkArgument(this.numPartitions > 0,
                                "numPartitions should be at least 1. Got %s", this.numPartitions);
  }

  @Override
  public int partition(String key, int numPartitions) {
    return key.hashCode() % this.numPartitions;
  }
}
