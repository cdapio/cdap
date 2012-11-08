package com.continuuity.api.common;

/**
 *
 */
public interface LogDispatcher {
  public static enum Level {
    TRACE,
    DEBUG,
    WARN,
    ERROR,
    INFO
  }
  public String getName();
  public Level getLevel();
  public void setLevel(Level level);
  public void trace(LogTag tag, String msg, Throwable throwable);
  public void trace(LogTag tag, String msg);
  public void debug(LogTag tag, String msg, Throwable throwable);
  public void debug(LogTag tag, String msg);
  public void warn(LogTag tag, String msg, Throwable throwable);
  public void warn(LogTag tag, String msg);
  public void error(LogTag tag, String msg, Throwable throwable);
  public void error(LogTag tag, String msg);
  public void info(LogTag tag, String msg, Throwable throwable);
  public void info(LogTag tag, String msg);
  public boolean isDebugEnabled();
  public boolean isInfoEnabled();
  public boolean isWarnEnabled();
  public boolean isTraceEnabled();
  public boolean isErrorEnabled();
  public boolean writeLog(LogTag tag, Level level, String message, String stack);
}
