/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.google.common.primitives.Longs;

import java.util.Comparator;

/**
 * A {@link Comparator} for {@link StreamEvent} that compares with event timestamp in ascending order.
 */
public final class StreamEventComparator implements Comparator<StreamEvent> {

  @Override
  public int compare(StreamEvent o1, StreamEvent o2) {
    return Longs.compare(o1.getTimestamp(), o2.getTimestamp());
  }
}
