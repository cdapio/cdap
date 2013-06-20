package com.continuuity.logging.tail;

import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Callback interface to receive logging events.
 */
public interface Callback {
  void handle(ILoggingEvent event);
}
