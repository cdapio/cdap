package com.continuuity.logging.filter;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Represents an expression that matches log level.
 */
public class LogLevelExpression implements Filter {
  private final Level level;

  public LogLevelExpression(String level) {
    this.level = Level.toLevel(level, Level.INFO);
  }

  @Override
  public boolean match(ILoggingEvent event) {
    return event.getLevel().isGreaterOrEqual(getLevel());
  }

  public Level getLevel() {
    return level;
  }
}
