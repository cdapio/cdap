/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A {@link StreamEvent} that forwards all methods to a another {@link StreamEvent}.
 */
public abstract class ForwardingStreamEvent implements StreamEvent {

  private final StreamEvent delegate;

  protected ForwardingStreamEvent(StreamEvent delegate) {
    this.delegate = delegate;
  }

  @Override
  public long getTimestamp() {
    return delegate.getTimestamp();
  }

  @Override
  public ByteBuffer getBody() {
    return delegate.getBody();
  }

  @Override
  public Map<String, String> getHeaders() {
    return delegate.getHeaders();
  }
}
