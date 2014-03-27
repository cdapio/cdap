/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

/**
 * Represents the configuration of a stream. This class needs to be GSON serializable.
 */
public final class StreamConfig {

  private final String name;
  private final long partitionDuration;
  private final long indexInterval;

  StreamConfig(String name, long partitionDuration, long indexInterval) {
    this.name = name;
    this.partitionDuration = partitionDuration;
    this.indexInterval = indexInterval;
  }

  public String getName() {
    return name;
  }

  public long getPartitionDuration() {
    return partitionDuration;
  }

  public long getIndexInterval() {
    return indexInterval;
  }
}
