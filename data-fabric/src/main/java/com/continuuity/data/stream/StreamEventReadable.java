/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 *
 * @param <OFFSET> The offset information type.
 */
public interface StreamEventReadable<OFFSET> extends Closeable {

  /**
   * Reads series of stream events up to the given maximum.
   *
   * @param events Collection to store the result.
   * @param maxEvents Maximum number of events to read.
   * @param timeout Maximum of time to spend on trying to read events
   * @param unit Unit for the timeout.
   *
   * @return Number of events read, potentially {@code 0}.
   *         If no more events due to end of file, {@code -1} is returned.
   * @throws IOException If there is IO error while reading.
   */
  int next(Collection<StreamEvent> events, int maxEvents,
           long timeout, TimeUnit unit) throws IOException, InterruptedException;

  /**
   * Returns current offset information.
   */
  OFFSET getOffset();
}
