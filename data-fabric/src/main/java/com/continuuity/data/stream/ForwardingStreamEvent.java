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
