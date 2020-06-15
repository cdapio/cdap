/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package io.cdap.cdap.logging.appender;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.internal.lang.CallerClassSecurityManager;
import io.cdap.cdap.logging.context.ApplicationLoggingContext;

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

  @Override
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
      addExtraTags(eventObject, loggingContext);
    }

    LogMessage logMessage = new LogMessage(eventObject, loggingContext);
    appendEvent(logMessage);
  }

  /**
   * Adds extra MDC tags to the given event.
   */
  private void addExtraTags(ILoggingEvent event, LoggingContext loggingContext) {
    // For error logs, if the logging context is in application scope, tag it as program logs.
    if (loggingContext.getSystemTagsMap().containsKey(ApplicationLoggingContext.TAG_APPLICATION_ID)
        && event.getLevel() == Level.ERROR) {
      event.getMDCPropertyMap().putAll(Collections.singletonMap(ORIGIN_KEY, "program"));
      return;
    }

    StackTraceElement[] callerData = event.getCallerData();
    if (callerData == null || callerData.length == 0) {
      return;
    }

    String callerClass = callerData[0].getClassName();

    Map<String, String> tags = loggerExtraTags.getIfPresent(callerClass);
    Class[] callerClasses = CallerClassSecurityManager.getCallerClasses();
    if (tags == null) {
      tags = getTagsForClass(callerClass, callerClasses);

      loggerExtraTags.put(callerClass, tags);
    }

    // If no tags were added using callerData, try using LoggerName
    if (tags.isEmpty()) {
      tags = loggerExtraTags.getIfPresent(event.getLoggerName());
      if (tags == null) {
        tags = getTagsForClass(event.getLoggerName(), callerClasses);
        loggerExtraTags.put(event.getLoggerName(), tags);
      }
    }

    // Add the extra tag to the log event
    event.getMDCPropertyMap().putAll(tags);
  }

  private Map<String, String> getTagsForClass(String className, Class[] callerClasses) {
    for (Class<?> cls : callerClasses) {
      if (cls.getName().equals(className)) {
        String classLoaderName = cls.getClassLoader().getClass().getName();
        switch (classLoaderName) {
          case "io.cdap.cdap.internal.app.runtime.plugin.PluginClassLoader":
            return Collections.singletonMap(ORIGIN_KEY, "plugin");
          case "io.cdap.cdap.internal.app.runtime.ProgramClassLoader":
            return Collections.singletonMap(ORIGIN_KEY, "program");
          default:
            return Collections.singletonMap(ORIGIN_KEY, "system");
        }
      }
    }
    return Collections.emptyMap();
  }

  protected abstract void appendEvent(LogMessage logMessage);

}
