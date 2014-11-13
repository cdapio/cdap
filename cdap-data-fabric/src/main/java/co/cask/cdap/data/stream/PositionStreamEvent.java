/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.data.stream;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.stream.StreamEventData;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A {@link StreamEvent} which also carries the file position where this event starts.
 */
class PositionStreamEvent extends StreamEvent {

  private final StreamEventData delegate;
  private final long timestamp;
  private final long position;

  PositionStreamEvent(StreamEventData delegate, long timestamp, long position) {
    this.delegate = delegate;
    this.timestamp = timestamp;
    this.position = position;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public ByteBuffer getBody() {
    return delegate.getBody();
  }

  @Override
  public Map<String, String> getHeaders() {
    return delegate.getHeaders();
  }

  public long getStart() {
    return position;
  }
}
