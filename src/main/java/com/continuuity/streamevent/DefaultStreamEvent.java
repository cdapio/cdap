package com.continuuity.streamevent;

import com.continuuity.api.flow.flowlet.StreamEvent;
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
public final class DefaultStreamEvent implements StreamEvent {

  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.wrap(new byte[0]);

  private final Map<String, String> headers;
  private final ByteBuffer body;

  public DefaultStreamEvent() {
    headers = ImmutableMap.of();
    body = EMPTY_BUFFER;
  }

  public DefaultStreamEvent(Map<String, String> headers, ByteBuffer body) {
    this.headers = ImmutableMap.copyOf(headers);
    this.body = body;
  }

  @Override
  public Map<String, String> getHeaders() {
    return headers;
  }

  @Override
  public ByteBuffer getBody() {
    return body;
  }
}
