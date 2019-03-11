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

package co.cask.cdap.logging.appender.loader;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.status.OnConsoleStatusListener;
import ch.qos.logback.core.status.StatusManager;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.elasticsearch.common.Strings;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

/**
 * Service that programmatically loads additional log appenders. It provides classloader isolation for loaded
 * appender so that appender can have dependencies that do not conflict with CDAP dependencies.
 */
public class LogAppenderLoaderService extends AbstractIdleService {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LogAppenderLoaderService.class);
  private final String appenderProvider;
  private final Appender<ILoggingEvent> logAppender;
  private final Map<String, String> properties;

  @SuppressWarnings("unchecked")
  @Inject
  public LogAppenderLoaderService(CConfiguration cConf) {
    this.properties = getProperties(cConf);
    this.appenderProvider = cConf.get(Constants.Logging.LOG_APPENDER_PROVIDER);
    if (Strings.isNullOrEmpty(appenderProvider)) {
      // If there is no provider, there is nothing to load.
      logAppender = null;
      return;
    }
    this.logAppender = new LogAppenderExtensionLoader("appender").get(appenderProvider);
  }

  @Override
  protected void startUp() throws Exception {
    if (Strings.isNullOrEmpty(appenderProvider)) {
      // If there is no provider, there is nothing to load. So return
      return;
    }

    if (logAppender == null) {
      // this will not happen unless log appender provider is misconfigured.
      throw new RuntimeException("Log appender " + appenderProvider + " is not constructed. " +
                                   "Please provide correct log appender provider.");
    }

    ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
    LoggerContext loggerContext = (LoggerContext) loggerFactory;
    if (loggerContext != null) {
      Logger logger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);

      // Check if the logger already contains the logAppender
      if (Iterators.contains(logger.iteratorForAppenders(), logAppender)) {
        return;
      }

      logAppender.setContext(loggerContext);
      // Display any errors during initialization of log appender to console
      configureStatusManager(loggerContext);

      ClassLoader oldClassLoader = getClass().getClassLoader();
      Thread.currentThread().setContextClassLoader(logAppender.getClass().getClassLoader());
      try {
        logAppender.setName(appenderProvider);
        Class<?> appenderClass = logAppender.getClass();
        // set all the provided properties for the log appender
        for (Map.Entry<String, String> property : properties.entrySet()) {
          appenderClass.getMethod(property.getKey(), String.class).invoke(logAppender, property.getValue());
        }

        logAppender.start();
      } finally {
        Thread.currentThread().setContextClassLoader(oldClassLoader);
      }

      logger.addAppender(logAppender);
    }

    LOG.info("Log Appender {} is initialized and started.", logAppender.getName());
  }

  private void configureStatusManager(LoggerContext loggerContext) {
    StatusManager statusManager = loggerContext.getStatusManager();
    OnConsoleStatusListener onConsoleListener = new OnConsoleStatusListener();
    statusManager.add(onConsoleListener);
  }

  @Override
  protected void shutDown() throws Exception {
    if (logAppender != null) {
      logAppender.stop();
    }
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
