/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.api.stream;

import java.nio.ByteBuffer;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Represents data in one stream event.
 */
@Nonnull
public interface StreamEventData {

  /**
   * @return A {@link java.nio.ByteBuffer} that is the payload of the event.
   */
  ByteBuffer getBody();

  /**
   * @return An immutable map of all headers included in this event.
   */
  Map<String, String> getHeaders();
}
