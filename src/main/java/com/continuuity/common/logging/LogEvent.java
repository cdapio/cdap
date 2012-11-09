package com.continuuity.common.logging;

import com.continuuity.api.common.LogDispatcher;
import com.continuuity.api.common.LogTag;

public class LogEvent {

  private final LogTag tag;
  private final LogDispatcher.Level level;
  private final String message;
  private final String stacktrace;

  public LogEvent(LogTag tag, LogDispatcher.Level level,
                  String message, String stacktrace) {
    this.tag = tag;
    this.level = level;
    this.message = message;
    this.stacktrace = stacktrace;
  }

  public LogTag getTag() {
    return tag;
  }

  public LogDispatcher.Level getLevel() {
    return level;
  }

  public String getMessage() {
    return message;
  }

  public String getStackTrace() {
    return stacktrace;
  }

}
