package com.continuuity.logging.read;

/**
 * Callback to handle log events.
 */
public interface Callback {
  /**
   * Called once at the beginning before calling @{link handle}.
   */
  void init();

  /**
   * Called for every log event.
   * @param event log event.
   */
  void handle(LogEvent event);

  /**
   * Called once at the end after all log events are done.
   */
  void close();
}
