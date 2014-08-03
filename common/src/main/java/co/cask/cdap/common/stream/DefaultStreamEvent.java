/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.common.stream;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.stream.StreamEventData;
import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Default implementation of {@link StreamEvent}.
 * <p/>
 * This class is temporary until the serialization API uses ASM for bytecode generation, as
 * this implementation would be generated on the fly by implementing the StreamEvent interface.
 */
@Nonnull
public final class DefaultStreamEvent extends DefaultStreamEventData implements StreamEvent {

  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.wrap(new byte[0]);

  private final long timestamp;

  public DefaultStreamEvent() {
    this(ImmutableMap.<String, String>of(), EMPTY_BUFFER);
  }

  public DefaultStreamEvent(Map<String, String> headers, ByteBuffer body) {
    this(headers, body, System.currentTimeMillis());
  }

  public DefaultStreamEvent(StreamEventData data, long timestamp) {
    this(data.getHeaders(), data.getBody(), timestamp);
  }

  public DefaultStreamEvent(Map<String, String> headers, ByteBuffer body, long timestamp) {
    super(headers, body);
    this.timestamp = timestamp;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }
}
