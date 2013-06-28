package com.continuuity.logging.tail;

import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Callback to handle log events.
 */
public interface Callback {
  void handle(ILoggingEvent event);
}
