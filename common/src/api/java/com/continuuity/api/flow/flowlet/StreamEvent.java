/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.api.flow.flowlet;

import com.continuuity.api.stream.StreamEventData;

import javax.annotation.Nonnull;

/**
 * Represents single event from a stream.
 *
 * TODO: Move this interface to com.continuuity.api.stream package.
 */
@Nonnull
public interface StreamEvent extends StreamEventData {

  /**
   * @return The timestamp in milliseconds for this event being injected.
   */
  long getTimestamp();
}
