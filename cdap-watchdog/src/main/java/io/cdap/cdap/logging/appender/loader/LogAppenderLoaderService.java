/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.logging.appender.loader;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.status.OnConsoleStatusListener;
import ch.qos.logback.core.status.StatusManager;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassLoaders;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * Service that programmatically loads additional log appenders. It provides classloader isolation for additional
 * appender.
 */
public class LogAppenderLoaderService extends AbstractIdleService {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LogAppenderLoaderService.class);
  private final CConfiguration cConf;
  private Appender<ILoggingEvent> logAppender;

  @Inject
  public LogAppenderLoaderService(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Override
  protected void startUp() throws Exception {
    String provider = cConf.get(Constants.Logging.LOG_APPENDER_PROVIDER);
    // If there is no provider, there is nothing to load. This can happen if no additional appenders are provided.
    if (Strings.isNullOrEmpty(provider)) {
      return;
    }

    logAppender = loadLogAppender(provider, cConf);
    initialize(logAppender, cConf);

    LOG.info("Log Appender {} is initialized and started.", provider);
  }

  @Override
  protected void shutDown() throws Exception {
    if (logAppender != null) {
      logAppender.stop();
      ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
      LoggerContext loggerContext = (LoggerContext) loggerFactory;
      if (loggerContext != null) {
        loggerContext.getLogger(Logger.ROOT_LOGGER_NAME).detachAppender(logAppender);
      }
    }
  }

  /**
   * Loads log appender using extension loader.
   */
  private Appender<ILoggingEvent> loadLogAppender(String appenderProvider, CConfiguration cConf) {
    Appender<ILoggingEvent> appender = new LogAppenderExtensionLoader(cConf).get(appenderProvider);
    if (appender == null) {
      // this will not happen unless log appender provider is misconfigured.
      throw new RuntimeException("Log appender " + appenderProvider + " is not constructed. " +
                                   "Please provide correct log appender provider.");
    }
    return appender;
  }

  /**
   * Initializes log appender by reading properties from cConf.
   */
  private void initialize(Appender<ILoggingEvent> appender, CConfiguration cConf) throws Exception {
    ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
    LoggerContext loggerContext = (LoggerContext) loggerFactory;
    if (loggerContext != null) {
      Logger logger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
      // Check if the logger already contains the logAppender
      if (!Iterators.contains(logger.iteratorForAppenders(), appender)) {
        appender.setContext(loggerContext);
        // Display any errors during initialization of log appender to console
        configureStatusManager(loggerContext);

        ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(appender.getClass().getClassLoader());
        try {
          Class<?> appenderClass = appender.getClass();
          // set all the provided properties for the log appender
          Map<String, String> properties = getProperties(cConf);
          for (Map.Entry<String, String> property : properties.entrySet()) {
            appenderClass.getMethod(property.getKey(), String.class).invoke(appender, property.getValue());
          }

          appender.start();
        } finally {
          Thread.currentThread().setContextClassLoader(oldClassLoader);
        }

        logger.addAppender(appender);
      }
    }
  }

  /**
   * Configures status manager.
   */
  private void configureStatusManager(LoggerContext loggerContext) {
    StatusManager statusManager = loggerContext.getStatusManager();
    OnConsoleStatusListener onConsoleListener = new OnConsoleStatusListener();
    statusManager.add(onConsoleListener);
  }

  /**
   * Gets all the properties of the log appender.
   */
  private Map<String, String> getProperties(CConfiguration cConf) {
    String prefix = String.format("%s%s.", Constants.Logging.LOG_APPENDER_PROPERTY_PREFIX,
                                  cConf.get(Constants.Logging.LOG_APPENDER_PROVIDER));
    return Collections.unmodifiableMap(cConf.getPropsWithPrefix(prefix));
  }
}
