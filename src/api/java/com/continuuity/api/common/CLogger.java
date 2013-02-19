package com.continuuity.api.common;

import java.util.Formatter;

/**
 *  Defines class to log data. Implements functions to log in different log levels.
 */
@Deprecated
public class CLogger {
  private static ThreadLocal<Formatter> formatterCache = new FormatterCache();
  private final LogDispatcher log;
  private final LogTag tag;

  /**
   * Static class for ThreadLocal Formatter Cache
   */
  private static class FormatterCache extends ThreadLocal<Formatter> {
    protected synchronized Formatter initialValue() {
      return new Formatter();
    }
  }

  /**
   * Constructor with tag and log dispatcher
   * @param tag tag
   * @param log logDispatcher
   */
  private CLogger(LogTag tag, LogDispatcher log) {
    this.tag = tag;
    this.log = log;
  }

  /**
   * Log debug message with msg and cause
   * @param msg log this message
   * @param throwable log this cause
   */
  public void debug(String msg, Throwable throwable) {
    log.debug(tag, msg, throwable);
  }

  /**
   * Log debug message
   * @param msg log this message
   */
  public void debug(String msg) {
    log.debug(tag, msg);
  }

  /**
   * Log error message with message and cause
   * @param msg log this error message
   * @param throwable log this cause
   */
  public void error(String msg, Throwable throwable) {
    log.error(tag, msg, throwable);
  }

  /**
   * Log error message
   * @param msg log this message
   */
  public void error(String msg) {
    log.error(tag, msg);
  }

  /**
   * Get name
   * @return String name
   */
  public String getName() {
    return log.getName();
  }

  /**
   * Log info message
   * @param s log this message
   * @param throwable log this cause
   */
  public void info(String s, Throwable throwable) {
    log.info(tag, s, throwable);
  }


  /**
   * Log info message
   * @param s log this message
   */
  public void info(String msg) {
    log.info(tag, msg);
  }

  /**
   * Check if debug level is enabled
   * @return true is debug level is enabled
   */
  public boolean isDebugEnabled() {
    return log.isDebugEnabled();
  }


  /**
   * Check if error is enabled
   * @return true is error level is enabled
   */
  public boolean isErrorEnabled() {
    return log.isErrorEnabled();
  }

  /**
   * Check if info level is enabled
   * @return true is info level is enabled
   */
  public boolean isInfoEnabled() {
    return log.isInfoEnabled();
  }

  /**
   * Check if trace level is enabled
   * @return true is trace level is enabled
   */
  public boolean isTraceEnabled() {
    return log.isTraceEnabled();
  }

  /**
   * Check if warning level is enabled
   * @return true is warning level is enabled
   */
  public boolean isWarnEnabled() {
    return log.isWarnEnabled();
  }

  /**
   * Log error with trace level
   * @param msg  Log this message
   * @param throwable log this cause
   */
  public void trace(String msg, Throwable throwable) {
    log.trace(tag, msg, throwable);
  }

  /**
   * Log error with trace level
   * @param msg log this message
   */
  public void trace(String msg) {
    log.trace(tag, msg);
  }

  /**
   * Log warning message
   * @param msg log this message
   * @param throwable log this cause
   */
  public void warn(String msg, Throwable throwable) {
    log.warn(tag, msg, throwable);
  }

  /**
   * Log warning message
   * @param msg log this message
   */
  public void warn(String msg) {
    log.warn(tag, msg);
  }

  /**
   * Log error with trace level
   * @param format Format specifier to format arguments
   * @param args log arguments with the specified format
   */
  public void trace(String format, Object... args) {
    if (!log.isTraceEnabled()) {
      return;
    }
    log.trace(tag, sprintf(format, args));
  }

  /**
   * Log error with trace level
   * @param format   Format specifier to format arguments
   * @param t   log this cause
   * @param args  log arguments with specified format
   */
  public void trace(String format, Throwable t, Object... args) {
    if (!log.isTraceEnabled()) {
      return;
    }
    log.trace(tag, sprintf(format, args), t);
  }


  /**
   * Log debug message
   * @param format   Format specifier to format arguments
   * @param args  log arguments with specified format
   */
  public void debug(String format, Object... args) {
    if (!log.isDebugEnabled()) {
      return;
    }
    log.debug(tag, sprintf(format, args));
  }


  /**
   * Log debug message
   * @param format   Format specifier to format arguments
   * @param t   log this cause
   * @param args  log arguments with specified format
   */
  public void debug(String format, Throwable t, Object... args) {
    if (!log.isDebugEnabled()) {
      return;
    }
    log.debug(tag, sprintf(format, args), t);
  }


  /**
   * Log info message
   * @param format   Format specifier to format arguments
   * @param args  log arguments with specified format
   */
  public void info(String format, Object... args) {
    if (!log.isInfoEnabled()) {
      return;
    }
    log.info(tag, sprintf(format, args));
  }


  /**
   * Log info message
   * @param format   Format specifier to format arguments
   * @param t   log this cause
   * @param args  log arguments with specified format
   */
  public void info(String format, Throwable t, Object... args) {
    if (!log.isInfoEnabled()) {
      return;
    }
    log.info(tag, sprintf(format, args), t);
  }


  /**
   * Log warning message
   * @param format   Format specifier to format arguments
   * @param t   log this cause
   * @param args  log arguments with specified format
   */
  public void warn(String format, Object... args) {
    if (!log.isWarnEnabled()) {
      return;
    }
    log.warn(tag, sprintf(format, args));
  }


  /**
   * Log warning message
   * @param format   Format specifier to format arguments
   * @param t   log this cause
   * @param args  log arguments with specified format
   */
  public void warn(String format, Throwable t, Object... args) {
    if (!log.isWarnEnabled()) {
      return;
    }
    log.warn(tag, sprintf(format, args), t);
  }


  /**
   * Log error message
   * @param format   Format specifier to format arguments
   * @param args  log arguments with specified format
   */
  public void error(String format, Object... args) {
    if (!log.isErrorEnabled()) {
      return;
    }
    log.error(tag, sprintf(format, args));
  }


  /**
   * Log error message
   * @param format   Format specifier to format arguments
   * @param t   log this cause
   * @param args  log arguments with specified format
   */
  public void error(String format, Throwable t, Object... args) {
    if (!log.isErrorEnabled()) {
      return;
    }
    log.error(tag, sprintf(format, args), t);
  }

  private static String sprintf(String format, Object[] args) {
    Formatter formatter = getFormatter();
    formatter.format(format, args);

    StringBuilder sb = (StringBuilder) formatter.out();
    String message = sb.toString();
    sb.setLength(0);

    return message;
  }

  private static Formatter getFormatter() {
    return formatterCache.get();
  }

  /**
   * Get logger instance
   * @param tagger LogTag
   * @param log LogDispatcher
   * @return Instance of {@code CLogger}
   */
  public static CLogger getLogger(LogTag tagger, LogDispatcher log) {
    return new CLogger(tagger, log);
  }

  public void traceMethodStart(Object... args) {
    if (!log.isTraceEnabled()) {
      return;
    }
    String method = Thread.currentThread().getStackTrace()[2].getMethodName();
    StringBuilder sb = new StringBuilder();
    sb.append(method);
    sb.append("(");
    for (int i = 0; i < args.length; i++) {
      sb.append(args[i]);
      if(i != args.length-1)
        sb.append(",");
    }
    sb.append(") is called");
    log.trace(tag, sb.toString());
  }

  public void traceMethodEnd() {
    if (!log.isTraceEnabled()) {
      return;
    }
    String method = Thread.currentThread().getStackTrace()[2].getMethodName();
    StringBuilder sb = new StringBuilder();
    sb.append(method);
    sb.append(" is returned");
    log.trace(tag, sb.toString());
  }
}
