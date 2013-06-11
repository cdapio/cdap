package com.continuuity.common.logging.logback.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * A simple partitioner based on String keys.
 */
@SuppressWarnings("UnusedDeclaration")
public class StringPartitioner implements Partitioner<String> {
  public StringPartitioner(VerifiableProperties props) {}

  @Override
  public int partition(String key, int numPartitions) {
    return key.hashCode() % numPartitions;
  }
}
