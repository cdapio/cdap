package com.continuuity.common.logging;

public class LogEvent {

  public static final String FIELD_NAME_LOGTAG = "logtag";
  public static final String FIELD_NAME_LOGLEVEL = "level";

  private final String tag;
  private final String level;
  private final String message;

  public LogEvent(String logtag, String level, String message) {
    this.tag = logtag;
    this.level = level;
    this.message = message;
  }

  public String getTag() {
    return tag;
  }

  public String getLevel() {
    return level;
  }

  public String getMessage() {
    return message;
  }

}
