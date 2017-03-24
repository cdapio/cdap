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

package co.cask.cdap.logging.filter;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Filter to only allow only User logs or errors from other sources
 */
public class CondensedLogFilter implements Filter {
  private static final Logger LOG = LoggerFactory.getLogger(CondensedLogFilter.class);
  private static final Map<String, Boolean> programClassCache = new HashMap<>();
  private static final String USER_LOG_TAG = ".userLog";
  private static final String TRUE_VALUE = "true";

  private static final MdcExpression mdcExpression = new MdcExpression(USER_LOG_TAG, TRUE_VALUE);
  private static final LogLevelExpression logLevelExpression = new LogLevelExpression(Level.ERROR.levelStr);
  private final boolean enabled;

  public CondensedLogFilter(String value) {
    this.enabled = value.equals("on");
  }

  public static void addUserLogTag(ILoggingEvent eventObject) {
    String className = eventObject.getCallerData()[0].getClassName();
    // Check if cache has the class
    if (programClassCache.get(className) != null) {
      if (programClassCache.get(className)) {
        eventObject.getMDCPropertyMap().put(USER_LOG_TAG, TRUE_VALUE);
      }
      return;
    }
    try {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader().loadClass(className).getClassLoader();
      if (classLoader == null || classLoader.toString().contains("ProgramClassLoader")) {
        eventObject.getMDCPropertyMap().put(USER_LOG_TAG, TRUE_VALUE);
        programClassCache.put(className, true);
      } else {
        programClassCache.put(className, false);
      }
    } catch (ClassNotFoundException e) {
      // should not happen
      LOG.error("Exception for class {}", className, e);
    }
  }

  @Override
  public boolean match(ILoggingEvent event) {
    return !enabled || mdcExpression.match(event) || logLevelExpression.match(event);
  }
}
