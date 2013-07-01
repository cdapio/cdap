package com.continuuity.logging.read;

import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Callback to handle log events.
 */
public interface Callback {
  void handle(ILoggingEvent event);
}
