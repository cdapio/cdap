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

package co.cask.cdap.api.flow.flowlet;

import co.cask.cdap.api.stream.StreamEventData;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Represents single event from a stream.
 *
 * TODO: Move this interface to co.cask.cdap.api.stream package.
 */
@Nonnull
public class StreamEvent extends StreamEventData {

  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.wrap(new byte[0]);

  private final long timestamp;

  /**
   * Creates an instance with empty body and empty headers.
   */
  public StreamEvent() {
    this(Collections.<String, String>emptyMap(), EMPTY_BUFFER);
  }

  /**
   * Creates an instance with the given body and empty headers.
   */
  public StreamEvent(ByteBuffer body) {
    this(Collections.<String, String>emptyMap(), body);
  }

  /**
   * Creates an instance with the given headers and body and current time as the event timestamp.
   */
  public StreamEvent(Map<String, String> headers, ByteBuffer body) {
    this(headers, body, System.currentTimeMillis());
  }

  /**
   * Creates an instance with the given {@link StreamEventData} and timestamp.
   */
  public StreamEvent(StreamEventData data, long timestamp) {
    this(data.getHeaders(), data.getBody(), timestamp);
  }

  /**
   * Creates an instance that copies from the another {@link StreamEvent}.
   */
  public StreamEvent(StreamEvent event) {
    this(event.getHeaders(), event.getBody(), event.getTimestamp());
  }

  /**
   * Creates an instance with the given headers, body and timestamp.
   */
  public StreamEvent(Map<String, String> headers, ByteBuffer body, long timestamp) {
    super(headers, body);
    this.timestamp = timestamp;
  }

  /**
   * @return The timestamp in milliseconds for this event being injected.
   */
  public long getTimestamp() {
    return timestamp;
  }
}
