package com.continuuity.common.logger;

import org.apache.log4j.Level;

/**
 *
 */
public interface LogDispatcher {
  public String getName();
  public Level getLevel();
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
}
