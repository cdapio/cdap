/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.google.common.base.Objects;
import org.apache.twill.filesystem.Location;

import javax.annotation.Nullable;

/**
 *
 */
public final class StreamFileOffset {

  private final Location eventLocation;
  private final Location indexLocation;
  private final long partitionStart;
  private final long partitionEnd;
  private final String namePrefix;
  private final int seqId;
  private long offset;

  /**
   * Clones from another {@link StreamFileOffset}.
   * @param other The instance to clone from.
   */
  public StreamFileOffset(StreamFileOffset other) {
    this(other.getEventLocation(), other.getIndexLocation(), other.getOffset());
  }

  public StreamFileOffset(Location eventLocation, @Nullable Location indexLocation) {
    this(eventLocation, indexLocation, 0L);
  }

  public StreamFileOffset(Location eventLocation, @Nullable Location indexLocation, long offset) {
    this.eventLocation = eventLocation;
    this.indexLocation = indexLocation;
    this.offset = offset;

    // See StreamInputFormat for the path format
    String partitionName = StreamUtils.getPartitionName(eventLocation.toURI());
    this.partitionStart = StreamUtils.getPartitionStartTime(partitionName);
    this.partitionEnd = StreamUtils.getPartitionEndTime(partitionName);

    this.namePrefix = StreamUtils.getNamePrefix(eventLocation.getName());
    this.seqId = StreamUtils.getSequenceId(eventLocation.getName());
  }

  public final Location getEventLocation() {
    return eventLocation;
  }

  public final Location getIndexLocation() {
    return indexLocation;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public final long getPartitionStart() {
    return partitionStart;
  }

  public final long getPartitionEnd() {
    return partitionEnd;
  }

  public final String getNamePrefix() {
    return namePrefix;
  }

  public final int getSequenceId() {
    return seqId;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("event", eventLocation.toURI())
      .add("index", indexLocation == null ? null : indexLocation.toURI())
      .add("offset", getOffset())
      .toString();
  }
}
