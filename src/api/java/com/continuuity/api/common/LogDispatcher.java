package com.continuuity.api.common;

/**
 * Log dispatcher
 */
public interface LogDispatcher {
  /**
   * Defines log level. TRACE, DEBUG, WARN, ERROR, INFO
   */
  public static enum Level {
    TRACE,
    DEBUG,
    WARN,
    ERROR,
    INFO
  }

  /**
   * Get name
   * @return String name
   */
  public String getName();

  /**
   * Get log level
   * @return {@code com.continuuity.api.common.LogDispatcher.Level }
   */
  public Level getLevel();

  /**
   * Set log level
   * @param level LogLevel
   */
  public void setLevel(Level level);

  /**
   * Log an error with trace level
   * @param tag Tag
   * @param msg log this message
   * @param throwable log this cause
   */
  public void trace(LogTag tag, String msg, Throwable throwable);

  /**
   * Log an error with trace level
   * @param tag Tag
   * @param msg log this message
   */
  public void trace(LogTag tag, String msg);

  /**
   * Log a debug message
   * @param tag Tag
   * @param msg log this message
   * @param throwable log this cause
   */
  public void debug(LogTag tag, String msg, Throwable throwable);

  /**
   * Log a debug message
   * @param tag Tag
   * @param msg log this message
   */
  public void debug(LogTag tag, String msg);


  /**
   * Log a warning message
   * @param tag Tag
   * @param msg log this message
   * @param throwable log this cause
   */
  public void warn(LogTag tag, String msg, Throwable throwable);


  /**
   * Log a warning message
   * @param tag Tag
   * @param msg log this message
   */
  public void warn(LogTag tag, String msg);


  /**
   * Log an error message
   * @param tag Tag
   * @param msg log this message
   * @param throwable log this cause
   */
  public void error(LogTag tag, String msg, Throwable throwable);


  /**
   * Log an error message
   * @param tag Tag
   * @param msg log this message
   */
  public void error(LogTag tag, String msg);

  /**
   * Log an info message
   * @param tag Tag
   * @param msg log this message
   * @param throwable log this cause
   */
  public void info(LogTag tag, String msg, Throwable throwable);


  /**
   * Log an info message
   * @param tag Tag
   * @param msg log this message
   */
  public void info(LogTag tag, String msg);


  /**
   * Return true if debug level is enabled
   * @return true if debug level is enabled, false otherwise
   */
  public boolean isDebugEnabled();

  /**
   * Return true if info level is enabled
   * @return true if info level is enabled, false otherwise
   */

  public boolean isInfoEnabled();

  /**
   * Return true if warning level is enabled
   * @return true if warning level is enabled, false otherwise
   */

  public boolean isWarnEnabled();
  /**
   * Return true if trace level is enabled
   * @return true if trace level is enabled, false otherwise
   */

  public boolean isTraceEnabled();
  /**
   * Return true if error level is enabled
   * @return true if eror level is enabled, false otherwise
   */
  public boolean isErrorEnabled();

  /**
   * Write a log message with a given log level
   * @param tag   tag
   * @param level log level
   * @param message log message
   * @param stack Stack trace
   * @return  true on successful write
   */
  public boolean writeLog(LogTag tag, Level level, String message, String stack);
}
