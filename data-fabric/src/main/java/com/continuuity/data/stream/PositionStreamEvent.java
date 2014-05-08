/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;

/**
 * A {@link StreamEvent} which also carries the file position where this event starts.
 */
public interface PositionStreamEvent extends StreamEvent {

  long getStart();
}
