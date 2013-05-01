/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow.flowlet;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Represents single event from a stream.
 */
@Nonnull
public interface StreamEvent {

  /**
   * @return An immutable map of all headers included in this event.
   */
  Map<String, String> getHeaders();

  /**
   * @return A {@link ByteBuffer} that is the payload of the event.
   */
  ByteBuffer getBody();
}
