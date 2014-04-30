/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.net.URI;

/**
 * Represents offset information for single stream file. The comparison of offset instance
 * is done by comparing event location path followed by offset.
 */
public final class StreamFileOffset implements Comparable<StreamFileOffset> {

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
    this(other.getEventLocation(), other.getOffset());
  }

  public StreamFileOffset(Location eventLocation) {
    this(eventLocation, 0L);
  }

  public StreamFileOffset(Location eventLocation, long offset) {
    Preconditions.checkNotNull(eventLocation, "Event file location cannot be null.");

    this.eventLocation = eventLocation;
    this.indexLocation = createIndexLocation(eventLocation);
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StreamFileOffset other = (StreamFileOffset) o;

    return (offset == other.offset) && Objects.equal(eventLocation, other.eventLocation);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(eventLocation, offset);
  }

  /**
   * Creates the index file location from the event file location.
   *
   * @param eventLocation Location for the event file.
   * @return Location of the index file.
   */
  private Location createIndexLocation(Location eventLocation) {
    LocationFactory factory = eventLocation.getLocationFactory();
    String eventPath = eventLocation.toURI().toString();
    int extLength = StreamFileType.EVENT.getSuffix().length();

    return factory.create(URI.create(String.format("%s%s",
                                                   eventPath.substring(0, eventPath.length() - extLength),
                                                   StreamFileType.INDEX.getSuffix())));
  }

  @Override
  public int compareTo(StreamFileOffset other) {
    int cmp = eventLocation.toURI().compareTo(other.getEventLocation().toURI());
    return cmp == 0 ? Longs.compare(offset, other.getOffset()) : cmp;
  }
}
