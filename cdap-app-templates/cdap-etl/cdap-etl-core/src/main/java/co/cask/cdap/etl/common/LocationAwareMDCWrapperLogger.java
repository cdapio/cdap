/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.etl.common;

import org.slf4j.Logger;
import org.slf4j.MDC;
import org.slf4j.Marker;
import org.slf4j.spi.LocationAwareLogger;

import javax.annotation.Nullable;

/**
 * {@link LocationAwareLogger} which wraps the emitting of log messages with MDC put and remove operations.
 */
public class LocationAwareMDCWrapperLogger implements Logger, LocationAwareLogger {

  private static final String FQCN = LocationAwareMDCWrapperLogger.class.getName();

  private final Logger logger;
  private final String mdcKey;
  private final String mdcValue;

  public LocationAwareMDCWrapperLogger(Logger logger, String mdcKey, String mdcValue) {
    this.logger = logger;
    this.mdcKey = mdcKey;
    this.mdcValue = mdcValue;
  }


  @Override
  public String getName() {
    return logger.getName();
  }

  @Override
  public boolean isTraceEnabled() {
    return logger.isTraceEnabled();
  }

  @Override
  public void trace(String msg) {
    trace((Marker) null, msg);
  }

  @Override
  public void trace(String format, Object arg) {
    trace((Marker) null, format, arg);
  }

  @Override
  public void trace(String format, Object arg1, Object arg2) {
    trace(null, format, arg1, arg2);
  }

  @Override
  public void trace(String format, Object... arguments) {
    trace((Marker) null, format, arguments);
  }

  @Override
  public void trace(String msg, Throwable t) {
    trace((Marker) null, msg, t);
  }

  @Override
  public boolean isTraceEnabled(Marker marker) {
    return logger.isTraceEnabled(marker);
  }

  @Override
  public void trace(Marker marker, String msg) {
    if (isTraceEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.TRACE_INT, msg, null, null);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public void trace(Marker marker, String format, Object arg) {
    if (isTraceEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.TRACE_INT, format, new Object[]{arg}, null);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public void trace(Marker marker, String format, Object arg1, Object arg2) {
    if (isTraceEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.TRACE_INT, format, new Object[]{arg1, arg2}, null);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public void trace(Marker marker, String format, Object... arguments) {
    if (isTraceEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.TRACE_INT, format, arguments, null);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public void trace(Marker marker, String msg, Throwable t) {
    if (isTraceEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.TRACE_INT, msg, null, t);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public boolean isDebugEnabled() {
    return logger.isDebugEnabled();
  }

  @Override
  public void debug(String msg) {
    debug((Marker) null, msg);
  }

  @Override
  public void debug(String format, Object arg) {
    debug((Marker) null, format, arg);
  }

  @Override
  public void debug(String format, Object arg1, Object arg2) {
    debug(null, format, arg1, arg2);
  }

  @Override
  public void debug(String format, Object... arguments) {
    debug((Marker) null, format, arguments);
  }

  @Override
  public void debug(String msg, Throwable t) {
    debug((Marker) null, msg, t);
  }

  @Override
  public boolean isDebugEnabled(Marker marker) {
    return logger.isDebugEnabled(marker);
  }

  @Override
  public void debug(Marker marker, String msg) {
    if (isDebugEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.DEBUG_INT, msg, null, null);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public void debug(Marker marker, String format, Object arg) {
    if (isDebugEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.DEBUG_INT, format, new Object[]{arg}, null);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public void debug(Marker marker, String format, Object arg1, Object arg2) {
    if (isDebugEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.DEBUG_INT, format, new Object[]{arg1, arg2}, null);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public void debug(Marker marker, String format, Object... arguments) {
    if (isDebugEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.DEBUG_INT, format, arguments, null);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public void debug(Marker marker, String msg, Throwable t) {
    if (isDebugEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.DEBUG_INT, msg, null, t);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public boolean isInfoEnabled() {
    return logger.isInfoEnabled();
  }

  @Override
  public void info(String msg) {
    info((Marker) null, msg);
  }

  @Override
  public void info(String format, Object arg) {
    info((Marker) null, format, arg);
  }

  @Override
  public void info(String format, Object arg1, Object arg2) {
    info(null, format, arg1, arg2);
  }

  @Override
  public void info(String format, Object... arguments) {
    info((Marker) null, format, arguments);
  }

  @Override
  public void info(String msg, Throwable t) {
    info((Marker) null, msg, t);
  }

  @Override
  public boolean isInfoEnabled(Marker marker) {
    return logger.isInfoEnabled(marker);
  }

  @Override
  public void info(Marker marker, String msg) {
    if (isInfoEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.INFO_INT, msg, null, null);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public void info(Marker marker, String format, Object arg) {
    if (isInfoEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.INFO_INT, format, new Object[]{arg}, null);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public void info(Marker marker, String format, Object arg1, Object arg2) {
    if (isInfoEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.INFO_INT, format, new Object[]{arg1, arg2}, null);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public void info(Marker marker, String format, Object... arguments) {
    if (isInfoEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.INFO_INT, format, arguments, null);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public void info(Marker marker, String msg, Throwable t) {
    if (isInfoEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.INFO_INT, msg, null, t);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public boolean isWarnEnabled() {
    return logger.isWarnEnabled();
  }

  @Override
  public void warn(String msg) {
    warn((Marker) null, msg);
  }

  @Override
  public void warn(String format, Object arg) {
    warn((Marker) null, format, arg);
  }

  @Override
  public void warn(String format, Object arg1, Object arg2) {
    warn(null, format, arg1, arg2);
  }

  @Override
  public void warn(String format, Object... arguments) {
    warn((Marker) null, format, arguments);
  }

  @Override
  public void warn(String msg, Throwable t) {
    warn((Marker) null, msg, t);
  }

  @Override
  public boolean isWarnEnabled(Marker marker) {
    return logger.isWarnEnabled(marker);
  }

  @Override
  public void warn(Marker marker, String msg) {
    if (isWarnEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.WARN_INT, msg, null, null);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public void warn(Marker marker, String format, Object arg) {
    if (isWarnEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.WARN_INT, format, new Object[]{arg}, null);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public void warn(Marker marker, String format, Object arg1, Object arg2) {
    if (isWarnEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.WARN_INT, format, new Object[]{arg1, arg2}, null);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public void warn(Marker marker, String format, Object... arguments) {
    if (isWarnEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.WARN_INT, format, arguments, null);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public void warn(Marker marker, String msg, Throwable t) {
    if (isWarnEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.WARN_INT, msg, null, t);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public boolean isErrorEnabled() {
    return logger.isErrorEnabled();
  }

  @Override
  public void error(String msg) {
    error((Marker) null, msg);
  }

  @Override
  public void error(String format, Object arg) {
    error((Marker) null, format, arg);
  }

  @Override
  public void error(String format, Object arg1, Object arg2) {
    error(null, format, arg1, arg2);
  }

  @Override
  public void error(String format, Object... arguments) {
    error((Marker) null, format, arguments);
  }

  @Override
  public void error(String msg, Throwable t) {
    error((Marker) null, msg, t);
  }

  @Override
  public boolean isErrorEnabled(Marker marker) {
    return logger.isErrorEnabled(marker);
  }

  @Override
  public void error(Marker marker, String msg) {
    if (isErrorEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.ERROR_INT, msg, null, null);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public void error(Marker marker, String format, Object arg) {
    if (isErrorEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.ERROR_INT, format, new Object[]{arg}, null);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public void error(Marker marker, String format, Object arg1, Object arg2) {
    if (isErrorEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.ERROR_INT, format, new Object[]{arg1, arg2}, null);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public void error(Marker marker, String format, Object... arguments) {
    if (isErrorEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.ERROR_INT, format, arguments, null);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public void error(Marker marker, String msg, Throwable t) {
    if (isErrorEnabled(marker)) {
      try {
        beforeLog();
        log(marker, FQCN, LocationAwareLogger.ERROR_INT, msg, null, t);
      } finally {
        afterLog();
      }
    }
  }

  @Override
  public void log(@Nullable Marker marker, String fqcn, int level,
                  String message, @Nullable Object[] args, @Nullable Throwable t) {
    if (logger instanceof LocationAwareLogger) {
      ((LocationAwareLogger) logger).log(marker, fqcn, level, message, args, t);
      return;
    }

    // This is getting a bit ugly as we have to call the specific log level method
    // Also we need to differential based on args and t.
    if (args == null) {
      switch (level) {
        case LocationAwareLogger.TRACE_INT:
          if (t == null) {
            logger.trace(marker, message);
          } else {
            logger.trace(marker, message, t);
          }
          break;
        case LocationAwareLogger.DEBUG_INT:
          if (t == null) {
            logger.debug(marker, message);
          } else {
            logger.debug(marker, message, t);
          }
          break;
        case LocationAwareLogger.INFO_INT:
          if (t == null) {
            logger.info(marker, message);
          } else {
            logger.info(marker, message, t);
          }
          break;
        case LocationAwareLogger.WARN_INT:
          if (t == null) {
            logger.warn(marker, message);
          } else {
            logger.warn(marker, message, t);
          }
          break;
        case LocationAwareLogger.ERROR_INT:
          if (t == null) {
            logger.error(marker, message);
          } else {
            logger.error(marker, message, t);
          }
          break;
        default:
          // This shouldn't happen. Just ignore it
      }
      return;
    }

    // If args is not null, need to append the throwable if it is not null
    Object[] arguments = args;
    if (t != null) {
      arguments = new Object[args.length + 1];
      System.arraycopy(args, 0, arguments, 0, args.length);
      arguments[args.length] = t;
    }
    switch (level) {
      case LocationAwareLogger.TRACE_INT:
        logger.trace(marker, message, arguments);
        break;
      case LocationAwareLogger.DEBUG_INT:
        logger.debug(marker, message, arguments);
        break;
      case LocationAwareLogger.INFO_INT:
        logger.info(marker, message, arguments);
        break;
      case LocationAwareLogger.WARN_INT:
        logger.warn(marker, message, arguments);
        break;
      case LocationAwareLogger.ERROR_INT:
        logger.error(marker, message, arguments);
        break;
      default:
        // This shouldn't happen. Just ignore it
    }
  }

  private void beforeLog() {
    MDC.put(mdcKey, mdcValue);
  }

  private void afterLog() {
    MDC.remove(mdcKey);
  }
}
