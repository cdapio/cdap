/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import org.apache.twill.filesystem.Location;

import javax.annotation.Nullable;

/**
 *
 */
public class StreamFileOffset {

  private final Location eventLocation;
  private final Location indexLocation;
  private final long offset;

  public StreamFileOffset(Location eventLocation, @Nullable Location indexLocation) {
    this(eventLocation, indexLocation, 0L);
  }

  public StreamFileOffset(Location eventLocation, @Nullable Location indexLocation, long offset) {
    this.eventLocation = eventLocation;
    this.indexLocation = indexLocation;
    this.offset = offset;
  }

  public Location getEventLocation() {
    return eventLocation;
  }

  public Location getIndexLocation() {
    return indexLocation;
  }

  public long getOffset() {
    return offset;
  }
}
