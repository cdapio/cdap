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

package io.cdap.cdap.logging.appender;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.status.OnConsoleStatusListener;
import ch.qos.logback.core.status.StatusManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Creates and sets the logback log appender.
 */
public class LogAppenderInitializer implements Closeable {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LogAppenderInitializer.class);
  private final Set<String> loggerNames = new HashSet<>();
  private final LogAppender logAppender;

  @Inject
  public LogAppenderInitializer(LogAppender logAppender) {
    this.logAppender = logAppender;
  }

  public void initialize() {
    initialize(Logger.ROOT_LOGGER_NAME);
  }

  @VisibleForTesting
  public synchronized void initialize(String loggerName) {
    LoggerContext loggerContext = getLoggerContext();
    if (loggerContext != null && loggerNames.add(loggerName)) {
      Logger logger = loggerContext.getLogger(loggerName);

      // Check if the logger already contains the logAppender
      if (Iterators.contains(logger.iteratorForAppenders(), logAppender)) {
        LOG.warn("Log appender {} is already initialized.", logAppender.getName());
        return;
      }

      logAppender.setContext(loggerContext);
      LOG.info("Initializing log appender {}", logAppender.getName());

      // Display any errors during initialization of log appender to console
      StatusManager statusManager = loggerContext.getStatusManager();
      OnConsoleStatusListener onConsoleListener = new OnConsoleStatusListener();
      statusManager.add(onConsoleListener);

      logAppender.start();

      logger.addAppender(logAppender);
    }
  }

  public void setLogLevels(Map<String, Level> logPairs) {
    LoggerContext loggerContext = getLoggerContext();
    if (loggerContext != null) {
      for (Map.Entry<String, Level> entry : logPairs.entrySet()) {
        String loggerName = entry.getKey();
        Level logLevel = entry.getValue();
        Logger logger = loggerContext.getLogger(loggerName);
        LOG.info("Log level of {} changed from {} to {}", loggerName, logger.getLevel(), logLevel);
        logger.setLevel(logLevel);
      }
    }
  }

  @Override
  public void close() {
    if (logAppender != null) {
      LOG.debug("Stopping log appender {}", logAppender.getName());
      logAppender.stop();
      LoggerContext loggerContext = getLoggerContext();
      if (loggerContext != null) {
        synchronized (this) {
          for (String loggerName : loggerNames) {
            loggerContext.getLogger(loggerName).detachAppender(logAppender);
          }
        }
      }
    }
  }
  
  /**
   * Helper function to get the {@link LoggerContext}
   * @return the {@link LoggerContext} or null if {@link LoggerFactory} is not a logback LoggerContext.
   */
  @Nullable
  private LoggerContext getLoggerContext() {
    ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
    if (!(loggerFactory instanceof LoggerContext)) {
      LOG.warn("LoggerFactory is not a logback LoggerContext. No log appender is added. " +
                 "Logback might not be in the classpath");
      return null;
    }
    return (LoggerContext) loggerFactory;
  }
}
