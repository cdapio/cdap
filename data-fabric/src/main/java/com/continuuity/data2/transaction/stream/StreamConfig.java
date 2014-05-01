/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.stream;

import com.google.common.base.Objects;
import org.apache.twill.filesystem.Location;

import javax.annotation.Nullable;

/**
 * Represents the configuration of a stream. This class needs to be GSON serializable.
 */
public final class StreamConfig {

  private final transient String name;
  private final long partitionDuration;
  private final long indexInterval;
  private final transient Location location;

  public StreamConfig(String name, long partitionDuration, long indexInterval, Location location) {
    this.name = name;
    this.partitionDuration = partitionDuration;
    this.indexInterval = indexInterval;
    this.location = location;
  }

  /**
   * @return Name of the stream.
   */
  public String getName() {
    return name;
  }

  /**
   * @return The duration in milliseconds that one partition in this stream contains.
   */
  public long getPartitionDuration() {
    return partitionDuration;
  }

  /**
   * @return The time interval in milliseconds that a new index entry would be created in the stream.
   */
  public long getIndexInterval() {
    return indexInterval;
  }

  /**
   * @return The location of the stream if it is file base stream, or {@code null} otherwise.
   */
  @Nullable
  public Location getLocation() {
    return location;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("name", name)
      .add("duration", partitionDuration)
      .add("indexInterval", indexInterval)
      .add("location", location)
      .toString();
  }
}
