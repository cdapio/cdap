/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
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
  private final long ttl;

  private final transient Location location;

  public StreamConfig(String name, long partitionDuration, long indexInterval, long ttl, Location location) {
    this.name = name;
    this.partitionDuration = partitionDuration;
    this.indexInterval = indexInterval;
    this.ttl = ttl;
    this.location = location;
  }

  public StreamConfig() {
    this.name = null;
    this.partitionDuration = 0;
    this.indexInterval = 0;
    this.ttl = Long.MAX_VALUE;
    this.location = null;
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
   * @return The time to live in milliseconds for events in this stream.
   */
  public long getTTL() {
    return ttl;
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
      .add("ttl", ttl)
      .add("location", location.toURI())
      .toString();
  }
}
