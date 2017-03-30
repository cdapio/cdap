/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.logging.appender;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CDAP log appender interface.
 */
public abstract class LogAppender extends AppenderBase<ILoggingEvent> {
  private static final String USER_LOG_TAG = ".userLog";
  private static final String USER_LOG_TRUE_VALUE = "true";

  public LogAppender() {
    System.out.println("Constructing LogAppender");
  }

  /**
   * Check classLoader of the log to add userTag
   */
  private static void addUserLogTag(ILoggingEvent eventObject) {
    String className = null;
    try {
      className = eventObject.getCallerData()[0].getClassName();
      ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
      if (contextClassLoader != null) {
        ClassLoader classLoader = contextClassLoader.loadClass(className).getClassLoader();
//          && ("co.cask.cdap.internal.app.runtime.plugin.ProgramClassLoader".equals(classLoader.getClass().getName())
//          || "co.cask.cdap.internal.app.runtime.plugin.PluginClassLoader".equals(classLoader.getClass().getName()))) {
          eventObject.getMDCPropertyMap().put(USER_LOG_TAG, classLoader.getClass().getName());
      }
    } catch (Throwable e) {
      if (className == null) {
        System.out.println("Failed to get class name for eventObject");
      } else {
        // should not happen
        System.out.println("Failed to mark user log for class " + className);
      }
      e.printStackTrace();
    }
  }

  public final void append(ILoggingEvent eventObject) {
    LoggingContext loggingContext;
    // If the context is not setup, pickup the context from thread-local.
    // If the context is already setup, use the context (in async mode).
    if (eventObject instanceof LogMessage) {
      loggingContext = ((LogMessage) eventObject).getLoggingContext();
    } else {
      loggingContext = LoggingContextAccessor.getLoggingContext();
      if (loggingContext == null) {
        return;
      }
    }
    // Check if this is a User Log and add appropriate tag
    addUserLogTag(eventObject);

    appendEvent(new LogMessage(eventObject, loggingContext));
  }

  protected abstract void appendEvent(LogMessage logMessage);

}
