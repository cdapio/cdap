/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;

/**
 * This class is a {@link StreamEvent} that also carries the corresponding {@link StreamFileOffset} that mark
 * the beginning offset of this stream event.
 */
public final class StreamEventOffset extends ForwardingStreamEvent {

  private final StreamFileOffset offset;

  public StreamEventOffset(StreamEvent event, StreamFileOffset offset) {
    super(event);
    this.offset = offset;
  }

  public StreamFileOffset getOffset() {
    return offset;
  }
}
