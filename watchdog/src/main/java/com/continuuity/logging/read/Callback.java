package com.continuuity.logging.read;

/**
 * Callback to handle log events.
 */
public interface Callback {
  void handle(LogEvent event);
  void close();
}
