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
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.AppenderBase;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.cdap.cdap.api.exception.ErrorCodeType;
import io.cdap.cdap.api.exception.FailureDetailsProvider;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.error.api.ErrorTagProvider;
import io.cdap.cdap.internal.lang.CallerClassSecurityManager;
import io.cdap.cdap.logging.context.ApplicationLoggingContext;
import io.cdap.cdap.logging.serialize.DelegatingLoggingEvent;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * CDAP log appender interface.
 */
public abstract class LogAppender extends AppenderBase<ILoggingEvent> {

  // Note, this constant is used in LoggingConfiguration.java
  @VisibleForTesting
  static final String ERROR_TAGS = "error.tags";
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
      //Creating a modifiable MDC to pass it to Delegating LoggingEvent
      Map<String, String> modifiableMDC = new HashMap<>(loggingContext.getSystemTagsAsString());
      modifiableMDC.putAll(eventObject.getMDCPropertyMap());
      eventObject = addExtraTags(eventObject, loggingContext, modifiableMDC);
    }

    LogMessage logMessage = new LogMessage(eventObject, loggingContext);
    appendEvent(logMessage);
  }

  /**
   * if event is for an exception deriving from ErrorCodeProvider, capture error group etc.
   */
  private void addErrorCodeTags(ILoggingEvent event, Map<String, String> modifiableMDC) {
    if (event.getThrowableProxy() == null
        || !(event.getThrowableProxy() instanceof ThrowableProxy)) {
      return;
    }
    Throwable throwable = ((ThrowableProxy) (event.getThrowableProxy())).getThrowable();
    // add all error tags from all the inner exceptions
    StringBuilder tagsSb = new StringBuilder();
    // keep track of all the tags from all inner exceptions in a set to ensure there are no duplicates in
    // the merged string
    Set<ErrorTagProvider.ErrorTag> combinedTags = new HashSet<>();
    while (throwable != null) {
      if (throwable instanceof ErrorTagProvider) {

        ErrorTagProvider errorCodeThrowable = (ErrorTagProvider) throwable;
        for (ErrorTagProvider.ErrorTag tag : errorCodeThrowable.getErrorTags()) {
          if (combinedTags.contains(tag)) {
            continue;
          }
          combinedTags.add(tag);

          if (tagsSb.length() > 0) {
            tagsSb.append(", ");
          }
          tagsSb.append(tag);
        }
      }
      throwable = throwable.getCause();
    }

    if (tagsSb.length() > 0) {
      modifiableMDC.put(ERROR_TAGS, tagsSb.toString());
    }
  }

  private void addErrorClassificationTags(ILoggingEvent event, Map<String, String> modifiableMdc) {
    if (event.getThrowableProxy() == null
        || !(event.getThrowableProxy() instanceof ThrowableProxy)) {
      return;
    }
    Throwable throwable = ((ThrowableProxy) (event.getThrowableProxy())).getThrowable();
    while (throwable != null) {

      if (throwable instanceof FailureDetailsProvider) {
        FailureDetailsProvider provider = (FailureDetailsProvider) throwable;
        boolean stageKeyAbsent = !modifiableMdc.containsKey(Constants.Logging.TAG_FAILED_STAGE);
        if (stageKeyAbsent && provider.getFailureStage() != null) {
          modifiableMdc.put(Constants.Logging.TAG_FAILED_STAGE, provider.getFailureStage());
        }
        modifiableMdc.put(Constants.Logging.TAG_ERROR_CATEGORY,
            provider.getErrorCategory().getErrorCategory());
        if (provider.getErrorReason() != null) {
          modifiableMdc.put(Constants.Logging.TAG_ERROR_REASON, provider.getErrorReason());
        }
        modifiableMdc.put(Constants.Logging.TAG_ERROR_TYPE, provider.getErrorType().name());
        modifiableMdc.put(Constants.Logging.TAG_DEPENDENCY, String.valueOf(
            provider.isDependency()));
        ErrorCodeType errorCodeType = provider.getErrorCodeType();
        String errorCode = provider.getErrorCode();
        String supportedDocUrl = provider.getSupportedDocumentationUrl();
        if (errorCodeType != null) {
          modifiableMdc.put(Constants.Logging.TAG_ERROR_CODE_TYPE, errorCodeType.name());
        }
        if (!Strings.isNullOrEmpty(errorCode)) {
          modifiableMdc.put(Constants.Logging.TAG_ERROR_CODE, errorCode);
        }
        if (!Strings.isNullOrEmpty(supportedDocUrl)) {
          modifiableMdc.put(Constants.Logging.TAG_SUPPORTED_DOC_URL, supportedDocUrl);
        }
      }
      throwable = throwable.getCause();
    }
  }

  /**
   * Adds extra MDC tags to the given event.
   */
  private ILoggingEvent addExtraTags(ILoggingEvent event, LoggingContext loggingContext,
      Map<String, String> modifiableMDC) {

    addErrorCodeTags(event, modifiableMDC);
    addErrorClassificationTags(event, modifiableMDC);

    // For error logs, if the logging context is in application scope, tag it as program logs.
    if (loggingContext.getSystemTagsMap().containsKey(ApplicationLoggingContext.TAG_APPLICATION_ID)
        && event.getLevel() == Level.ERROR) {
      modifiableMDC.put(ORIGIN_KEY, "program");
      return new DelegatingLoggingEvent(event, modifiableMDC);
    }

    StackTraceElement[] callerData = event.getCallerData();
    if (callerData == null || callerData.length == 0) {
      return new DelegatingLoggingEvent(event, modifiableMDC);
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
    modifiableMDC.putAll(tags);

    return new DelegatingLoggingEvent(event, modifiableMDC);
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
