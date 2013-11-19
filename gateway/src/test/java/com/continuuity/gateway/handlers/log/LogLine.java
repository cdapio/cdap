package com.continuuity.gateway.handlers.log;

/**
* Test Log object.
*/
class LogLine {
  private final long offset;
  private final String log;

  LogLine(long offset, String log) {
    this.offset = offset;
    this.log = log;
  }

  public long getOffset() {
    return offset;
  }

  public String getLog() {
    return log;
  }

  @Override
  public String toString() {
    return "LogLine{" +
      "offset=" + offset +
      ", log='" + log + '\'' +
      '}';
  }
}
