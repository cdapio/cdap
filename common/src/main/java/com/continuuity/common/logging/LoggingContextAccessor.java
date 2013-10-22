/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.common.logging;

/**
 * Allows to store and access the logging context.
 * <p>
 *   The logging context is injected into log messages emitted via standard logging APIs. This enables grouping logs
 *   based on the execution context of the place where log message was emitted and searching messages on the logs
 *   processing back-end.
 * </p>
 */
public class LoggingContextAccessor {
  private static final InheritableThreadLocal<LoggingContext> loggingContext =
    new InheritableThreadLocal<LoggingContext>();

  /**
   * Sets the logging context.
   * <p>
   *   NOTE: in work execution frameworks where threads are shared between workers (like Akka) we would have to init
   *         context very frequently (before every chunk of work is started). In that case we really want to re-use
   *         logging context object instance.
   * </p>
   * @param context context to set
   */
  public static void setLoggingContext(LoggingContext context) {
    loggingContext.set(context);
  }

  /**
   * @return LoggingContext if it was set. Returns null otherwise.
   */
  public static LoggingContext getLoggingContext() {
    return loggingContext.get();
  }
}
