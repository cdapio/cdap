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

package co.cask.cdap.logging.framework;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.util.LoggerNameUtil;

/**
 * Utility class for interacting with logback {@link LoggerContext} and {@link Logger}.
 */
public final class Loggers {

  /**
   * Returns the effective {@link Logger} for the given logger name. An
   * effective {@link Logger} is the most specific {@link Logger} in the logger
   * hierarchy based on the given logger name, which is already defined in the given {@link LoggerContext}.
   */
  public static Logger getEffectiveLogger(LoggerContext context, String loggerName) {
    // If there is such logger, then just return the effective level
    Logger logger = context.exists(loggerName);
    if (logger != null) {
      return logger;
    }

    // Otherwise, search through the logger hierarchy. We don't call loggerContext.getLogger to avoid
    // caching of the logger inside the loggerContext, which there is no guarantee on the memory usage
    // and there is no way to clean it up.
    logger = context.getLogger(Logger.ROOT_LOGGER_NAME);
    int idx = LoggerNameUtil.getFirstSeparatorIndexOf(loggerName);
    while (idx >= 0) {
      Logger parentLogger = context.exists(loggerName.substring(0, idx));
      if (parentLogger != null) {
        logger = parentLogger;
      }
      idx = LoggerNameUtil.getSeparatorIndexOf(loggerName, idx + 1);
    }

    // Returns the most specific logger for the given logger name
    return logger;
  }

  private Loggers() {
    // no-op
  }
}
