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
import co.cask.cdap.internal.lang.CallerClassSecurityManager;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * CDAP log appender interface.
 */
public abstract class LogAppender extends AppenderBase<ILoggingEvent> {

  private static final String ORIGIN_KEY = ".origin";
  private static final int LOGGER_CACHE_SIZE = 1000;
  private static final long LOGGER_CACHE_EXPIRY_MILLIS = 60000;

  private final Cache<String, Map<String, String>> loggerExtraTags;

  protected LogAppender() {
    this.loggerExtraTags = CacheBuilder
      .newBuilder()
      .maximumSize(LOGGER_CACHE_SIZE)
      .expireAfterAccess(LOGGER_CACHE_EXPIRY_MILLIS, TimeUnit.MILLISECONDS)
      .build();
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

    LogMessage logMessage = new LogMessage(eventObject, loggingContext);
    addExtraTags(logMessage);
    appendEvent(logMessage);
  }

  /**
   * Adds extra MDC tags to the given event.
   */
  private void addExtraTags(ILoggingEvent event) {
    StackTraceElement[] callerData = event.getCallerData();
    if (callerData == null || callerData.length == 0) {
      return;
    }

    String callerClass = callerData[0].getClassName();

    Map<String, String> tags = loggerExtraTags.getIfPresent(callerClass);
    if (tags == null) {
      tags = Collections.emptyMap();

      for (Class<?> cls : CallerClassSecurityManager.getCallerClasses()) {
        if (cls.getName().equals(callerClass)) {
          String classLoaderName = cls.getClassLoader().getClass().getName();
          switch (classLoaderName) {
            case "co.cask.cdap.internal.app.runtime.plugin.PluginClassLoader":
              tags = Collections.singletonMap(ORIGIN_KEY, "plugin");
              break;
            case "co.cask.cdap.internal.app.runtime.ProgramClassLoader":
              tags = Collections.singletonMap(ORIGIN_KEY, "program");
              break;
            default:
              tags = Collections.singletonMap(ORIGIN_KEY, "system");
          }
          break;
        }
      }

      loggerExtraTags.put(callerClass, tags);
    }

    // Add the extra tag to the log event
    Map<String, String> mdc = event.getMDCPropertyMap();
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      mdc.put(entry.getKey(), entry.getValue());
    }
  }

  protected abstract void appendEvent(LogMessage logMessage);

}
