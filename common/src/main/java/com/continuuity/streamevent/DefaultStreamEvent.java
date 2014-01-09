package com.continuuity.streamevent;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.stream.StreamData;
import com.continuuity.common.stream.DefaultStreamData;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Default implementation of {@link StreamEvent}.
 * <p/>
 * This class is temporary until the serialization API uses ASM for bytecode generation, as
 * this implementation would be generated on the fly by implementing the StreamEvent interface.
 */
@Nonnull
public final class DefaultStreamEvent extends DefaultStreamData implements StreamEvent {

  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.wrap(new byte[0]);

  private final long timestamp;

  public DefaultStreamEvent() {
    this(ImmutableMap.<String, String>of(), EMPTY_BUFFER);
  }

  public DefaultStreamEvent(Map<String, String> headers, ByteBuffer body) {
    this(headers, body, System.currentTimeMillis());
  }

  public DefaultStreamEvent(StreamData data, long timestamp) {
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
