/*
 * Copyright © 2015 Cask Data, Inc.
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
import org.slf4j.Marker;

/**
 * A Logger that prefixes every message with the name of the stage it was logged from.
 *
 * This is used instead of including the stage name in the Logger name in order to prevent certain stage names from
 * being shortened by the Logger.
 * For example, if we just do LoggerFactory.getLogger(this.getClass().getName() + ":" + stageName),
 * if the stageName contains a '.', everything to the left of the dot will get shortened.
 */
public class StageLogger implements Logger {
  private final Logger logger;
  private final String prefix;

  public StageLogger(Logger logger, String stageName) {
    this.logger = logger;
    this.prefix = stageName + " - ";
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
    logger.trace(prefix + msg);
  }

  @Override
  public void trace(String format, Object arg) {
    logger.trace(prefix + format, arg);
  }

  @Override
  public void trace(String format, Object arg1, Object arg2) {
    logger.trace(prefix + format, arg1, arg2);
  }

  @Override
  public void trace(String format, Object... arguments) {
    logger.trace(prefix + format, arguments);
  }

  @Override
  public void trace(String msg, Throwable t) {
    logger.trace(prefix + msg, t);
  }

  @Override
  public boolean isTraceEnabled(Marker marker) {
    return logger.isTraceEnabled(marker);
  }

  @Override
  public void trace(Marker marker, String msg) {
    logger.trace(marker, prefix + msg);
  }

  @Override
  public void trace(Marker marker, String format, Object arg) {
    logger.trace(marker, prefix + format, arg);
  }

  @Override
  public void trace(Marker marker, String format, Object arg1, Object arg2) {
    logger.trace(marker, prefix + format, arg1, arg2);
  }

  @Override
  public void trace(Marker marker, String format, Object... argArray) {
    logger.trace(marker, prefix + format, argArray);
  }

  @Override
  public void trace(Marker marker, String msg, Throwable t) {
    logger.trace(marker, prefix + msg, t);
  }

  @Override
  public boolean isDebugEnabled() {
    return logger.isDebugEnabled();
  }

  @Override
  public void debug(String msg) {
    logger.debug(prefix + msg);
  }

  @Override
  public void debug(String format, Object arg) {
    logger.debug(prefix + format, arg);
  }

  @Override
  public void debug(String format, Object arg1, Object arg2) {
    logger.debug(prefix + format, arg1, arg2);
  }

  @Override
  public void debug(String format, Object... arguments) {
    logger.debug(prefix + format, arguments);
  }

  @Override
  public void debug(String msg, Throwable t) {
    logger.debug(prefix + msg, t);
  }

  @Override
  public boolean isDebugEnabled(Marker marker) {
    return logger.isDebugEnabled(marker);
  }

  @Override
  public void debug(Marker marker, String msg) {
    logger.debug(marker, prefix + msg);
  }

  @Override
  public void debug(Marker marker, String format, Object arg) {
    logger.debug(marker, prefix + format, arg);
  }

  @Override
  public void debug(Marker marker, String format, Object arg1, Object arg2) {
    logger.debug(marker, prefix + format, arg1, arg2);
  }

  @Override
  public void debug(Marker marker, String format, Object... arguments) {
    logger.debug(marker, prefix + format, arguments);
  }

  @Override
  public void debug(Marker marker, String msg, Throwable t) {
    logger.debug(marker, prefix + msg, t);
  }

  @Override
  public boolean isInfoEnabled() {
    return logger.isInfoEnabled();
  }

  @Override
  public void info(String msg) {
    logger.info(prefix + msg);
  }

  @Override
  public void info(String format, Object arg) {
    logger.info(prefix + format, arg);
  }

  @Override
  public void info(String format, Object arg1, Object arg2) {
    logger.info(prefix + format, arg1, arg2);
  }

  @Override
  public void info(String format, Object... arguments) {
    logger.info(prefix + format, arguments);
  }

  @Override
  public void info(String msg, Throwable t) {
    logger.info(prefix + msg, t);
  }

  @Override
  public boolean isInfoEnabled(Marker marker) {
    return logger.isInfoEnabled(marker);
  }

  @Override
  public void info(Marker marker, String msg) {
    logger.info(marker, prefix + msg);
  }

  @Override
  public void info(Marker marker, String format, Object arg) {
    logger.info(marker, prefix + format, arg);
  }

  @Override
  public void info(Marker marker, String format, Object arg1, Object arg2) {
    logger.info(marker, prefix + format, arg1, arg2);
  }

  @Override
  public void info(Marker marker, String format, Object... arguments) {
    logger.info(marker, prefix + format, arguments);
  }

  @Override
  public void info(Marker marker, String msg, Throwable t) {
    logger.info(marker, prefix + msg, t);
  }

  @Override
  public boolean isWarnEnabled() {
    return logger.isWarnEnabled();
  }

  @Override
  public void warn(String msg) {
    logger.warn(prefix + msg);
  }

  @Override
  public void warn(String format, Object arg) {
    logger.warn(prefix + format, arg);
  }

  @Override
  public void warn(String format, Object... arguments) {
    logger.warn(prefix + format, arguments);
  }

  @Override
  public void warn(String format, Object arg1, Object arg2) {
    logger.warn(prefix + format, arg1, arg2);
  }

  @Override
  public void warn(String msg, Throwable t) {
    logger.warn(prefix + msg, t);
  }

  @Override
  public boolean isWarnEnabled(Marker marker) {
    return logger.isWarnEnabled(marker);
  }

  @Override
  public void warn(Marker marker, String msg) {
    logger.warn(marker, prefix + msg);
  }

  @Override
  public void warn(Marker marker, String format, Object arg) {
    logger.warn(marker, prefix + format, arg);
  }

  @Override
  public void warn(Marker marker, String format, Object arg1, Object arg2) {
    logger.warn(marker, prefix + format, arg1, arg2);
  }

  @Override
  public void warn(Marker marker, String format, Object... arguments) {
    logger.warn(marker, prefix + format, arguments);
  }

  @Override
  public void warn(Marker marker, String msg, Throwable t) {
    logger.warn(marker, prefix + msg, t);
  }

  @Override
  public boolean isErrorEnabled() {
    return logger.isErrorEnabled();
  }

  @Override
  public void error(String msg) {
    logger.error(prefix + msg);
  }

  @Override
  public void error(String format, Object arg) {
    logger.error(prefix + format, arg);
  }

  @Override
  public void error(String format, Object arg1, Object arg2) {
    logger.error(prefix + format, arg1, arg2);
  }

  @Override
  public void error(String format, Object... arguments) {
    logger.error(prefix + format, arguments);
  }

  @Override
  public void error(String msg, Throwable t) {
    logger.error(prefix + msg, t);
  }

  @Override
  public boolean isErrorEnabled(Marker marker) {
    return logger.isErrorEnabled(marker);
  }

  @Override
  public void error(Marker marker, String msg) {
    logger.error(marker, prefix + msg);
  }

  @Override
  public void error(Marker marker, String format, Object arg) {
    logger.error(marker, prefix + format, arg);
  }

  @Override
  public void error(Marker marker, String format, Object arg1, Object arg2) {
    logger.error(marker, prefix + format, arg1, arg2);
  }

  @Override
  public void error(Marker marker, String format, Object... arguments) {
    logger.error(marker, prefix + format, arguments);
  }

  @Override
  public void error(Marker marker, String msg, Throwable t) {
    logger.error(marker, prefix + msg, t);
  }
}
