package com.continuuity.common.logging;

import com.continuuity.api.common.LogDispatcher;
import com.continuuity.api.common.LogTag;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.StackTraceUtil;

/**
 * Created with IntelliJ IDEA.
 * User: nmotgi
 * Date: 11/8/12
 * Time: 1:40 PM
 * To change this template use File | Settings | File Templates.
 */
@Deprecated
public abstract class AbstractLogDispatcher implements LogDispatcher {
  private Level level = Level.INFO;

  @Override
  public Level getLevel() {
    return level;
  }

  @Override
  public void setLevel(Level level) {
    this.level = level;
  }

  private void sendToFlume(LogTag tag, Level level, String message) {
    writeLog(tag, level, message, null);
  }

  @Override
  public void trace(LogTag tag, String msg, Throwable throwable) {
    writeLog(tag, Level.TRACE, msg, StackTraceUtil.toStringStackTrace(throwable));
  }

  @Override
  public void trace(LogTag tag, String msg) {
    sendToFlume(tag, Level.TRACE, msg);
  }

  @Override
  public void debug(LogTag tag, String msg, Throwable throwable) {
    writeLog(tag, Level.DEBUG, msg, StackTraceUtil.toStringStackTrace
      (throwable));
  }

  @Override
  public void debug(LogTag tag, String msg) {
    writeLog(tag, Level.DEBUG, msg, null);
  }

  @Override
  public void warn(LogTag tag, String msg, Throwable throwable) {
    writeLog(tag, Level.WARN, msg,
             StackTraceUtil.toStringStackTrace(throwable));
  }

  @Override
  public void warn(LogTag tag, String msg) {
    writeLog(tag, Level.WARN, msg, null);
  }

  @Override
  public void error(LogTag tag, String msg, Throwable throwable) {
    writeLog(tag, Level.ERROR, msg,
             StackTraceUtil.toStringStackTrace(throwable));
  }

  @Override
  public void error(LogTag tag, String msg) {
    writeLog(tag, Level.ERROR, msg, null);
  }

  @Override
  public void info(LogTag tag, String msg, Throwable throwable) {
    writeLog(tag, Level.INFO, msg,
             StackTraceUtil.toStringStackTrace(throwable));
  }

  @Override
  public void info(LogTag tag, String msg) {
    writeLog(tag, Level.INFO, msg, null);
  }

  @Override
  public boolean isDebugEnabled() {
    return level == Level.DEBUG;
  }

  @Override
  public boolean isInfoEnabled() {
    return level == Level.INFO;
  }

  @Override
  public boolean isWarnEnabled() {
    return level == Level.WARN;
  }

  @Override
  public boolean isTraceEnabled() {
    return level == Level.TRACE;
  }

  @Override
  public boolean isErrorEnabled() {
    return level == Level.ERROR;
  }

}
