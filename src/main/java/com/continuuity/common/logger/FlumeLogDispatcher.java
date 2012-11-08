package com.continuuity.common.logger;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.StackTraceUtil;
import org.apache.log4j.Level;


/**
 *
 */
public class FlumeLogDispatcher implements LogDispatcher {

  private CConfiguration configuration;

  public FlumeLogDispatcher(CConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public String getName() {
    return null;  //To change body of implemented methods use File | Settings
    // | File Templates.
  }

  @Override
  public Level getLevel() {
    return null;  //To change body of implemented methods use File | Settings
    // | File Templates.
  }

  private void sendToFlume(LogTag tag, Level level, String message) {
    sendToFlume(tag, level, message, null);
  }

  private void sendToFlume(LogTag tag, Level level,
                           String message, String stack) {
    if(stack != null) {

    }
  }

  @Override
  public void trace(LogTag tag, String msg, Throwable throwable) {
    sendToFlume(tag, Level.TRACE,
                msg, StackTraceUtil.toStringStackTrace(throwable));
  }

  @Override
  public void trace(LogTag tag, String msg) {
    sendToFlume(tag, Level.TRACE, msg);
  }

  @Override
  public void debug(LogTag tag, String msg, Throwable throwable) {
    sendToFlume(tag, Level.DEBUG, msg,
                StackTraceUtil.toStringStackTrace(throwable));
  }

  @Override
  public void debug(LogTag tag, String msg) {

  }

  @Override
  public void warn(LogTag tag, String msg, Throwable throwable) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void warn(LogTag tag, String msg) {
    //To change body of implemented methods use File | Settings | File
    // Templates.
  }

  @Override
  public void error(LogTag tag, String msg, Throwable throwable) {
    //To change body of implemented methods use File | Settings | File
    // Templates.
  }

  @Override
  public void error(LogTag tag, String msg) {
    //To change body of implemented methods use File | Settings | File
    // Templates.
  }

  @Override
  public void info(LogTag tag, String msg, Throwable throwable) {
    //To change body of implemented methods use File | Settings | File
    // Templates.
  }

  @Override
  public void info(LogTag tag, String msg) {
    //To change body of implemented methods use File | Settings | File
    // Templates.
  }

  @Override
  public boolean isDebugEnabled() {
    return false;  //To change body of implemented methods use File |
    // Settings | File Templates.
  }

  @Override
  public boolean isInfoEnabled() {
    return false;  //To change body of implemented methods use File |
    // Settings | File Templates.
  }

  @Override
  public boolean isWarnEnabled() {
    return false;  //To change body of implemented methods use File |
    // Settings | File Templates.
  }

  @Override
  public boolean isTraceEnabled() {
    return false;  //To change body of implemented methods use File |
    // Settings | File Templates.
  }

  @Override
  public boolean isErrorEnabled() {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
