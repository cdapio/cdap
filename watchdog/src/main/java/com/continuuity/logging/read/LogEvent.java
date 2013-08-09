package com.continuuity.logging.read;

import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Represents a log event returned by LogReader.
 */
public class LogEvent {
  private final ILoggingEvent loggingEvent;
  private final long offset;

  public LogEvent(ILoggingEvent loggingEvent, long offset) {
    this.loggingEvent = loggingEvent;
    this.offset = offset;
  }

  public ILoggingEvent getLoggingEvent() {
    return loggingEvent;
  }

  public long getOffset() {
    return offset;
  }
}
