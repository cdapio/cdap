package com.continuuity.common.logger;

import java.util.Formatter;

/**
 *
 */
public class CLogger {
  private static ThreadLocal<Formatter> formatterCache = new FormatterCache();
  private final LogDispatcher log;
  private final LogTag tag;

  private static class FormatterCache extends ThreadLocal<Formatter> {
    protected synchronized Formatter initialValue() {
      return new Formatter();
    }
  }

  private CLogger(LogTag tag, LogDispatcher log) {
    this.tag = tag;
    this.log = log;
  }

  public void debug(String msg, Throwable throwable) {
    log.debug(tag, msg, throwable);
  }

  public void debug(String msg) {
    log.debug(tag, msg);
  }

  public void error(String msg, Throwable throwable) {
    log.error(tag, msg, throwable);
  }

  public void error(String msg) {
    log.error(tag, msg);
  }

  public String getName() {
    return log.getName();
  }

  public void info(String s, Throwable throwable) {
    log.info(tag, s, throwable);
  }

  public void info(String msg) {
    log.info(tag, msg);
  }

  public boolean isDebugEnabled() {
    return log.isDebugEnabled();
  }

  public boolean isErrorEnabled() {
    return log.isErrorEnabled();
  }

  public boolean isInfoEnabled() {
    return log.isInfoEnabled();
  }

  public boolean isTraceEnabled() {
    return log.isTraceEnabled();
  }

  public boolean isWarnEnabled() {
    return log.isWarnEnabled();
  }

  public void trace(String msg, Throwable throwable) {
    log.trace(tag, msg, throwable);
  }

  public void trace(String msg) {
    log.trace(tag, msg);
  }

  public void warn(String msg, Throwable throwable) {
    log.warn(tag, msg, throwable);
  }

  public void warn(String msg) {
    log.warn(tag, msg);
  }

  public void trace(String format, Object... args) {
    if (!log.isTraceEnabled()) {
      return;
    }
    log.trace(tag, sprintf(format, args));
  }

  public void trace(String format, Throwable t, Object... args) {
    if (!log.isTraceEnabled()) {
      return;
    }
    log.trace(tag, sprintf(format, args), t);
  }

  public void debug(String format, Object... args) {
    if (!log.isDebugEnabled()) {
      return;
    }
    log.debug(tag, sprintf(format, args));
  }

  public void debug(String format, Throwable t, Object... args) {
    if (!log.isDebugEnabled()) {
      return;
    }
    log.debug(tag, sprintf(format, args), t);
  }

  public void info(String format, Object... args) {
    if (!log.isInfoEnabled()) {
      return;
    }
    log.info(tag, sprintf(format, args));
  }

  public void info(String format, Throwable t, Object... args) {
    if (!log.isInfoEnabled()) {
      return;
    }
    log.info(tag, sprintf(format, args), t);
  }

  public void warn(String format, Object... args) {
    if (!log.isWarnEnabled()) {
      return;
    }
    log.warn(tag, sprintf(format, args));
  }

  public void warn(String format, Throwable t, Object... args) {
    if (!log.isWarnEnabled()) {
      return;
    }
    log.warn(tag, sprintf(format, args), t);
  }

  public void error(String format, Object... args) {
    if (!log.isErrorEnabled()) {
      return;
    }
    log.error(tag, sprintf(format, args));
  }

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
