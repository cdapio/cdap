package com.continuuity.logging.appender;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.status.OnConsoleStatusListener;
import ch.qos.logback.core.status.StatusManager;
import com.google.inject.Inject;
import org.slf4j.LoggerFactory;

/**
 * Creates and sets the logback log appender.
 */
public class LogAppenderInitializer {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LogAppenderInitializer.class);
  private final LogAppender logAppender;

  @Inject
  public LogAppenderInitializer(LogAppender logAppender) {
    this.logAppender = logAppender;
  }

  public void initialize() {
    initialize(org.slf4j.Logger.ROOT_LOGGER_NAME);
  }

  public void initialize(String name) {
    LOG.info("Initializing log appender {}", logAppender.getName());

    LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();

    // Display any errors during initialization of log appender to console
    StatusManager statusManager = loggerContext.getStatusManager();
    OnConsoleStatusListener onConsoleListener = new OnConsoleStatusListener();
    statusManager.add(onConsoleListener);

    logAppender.setContext(loggerContext);
    logAppender.start();

    Logger rootLogger = loggerContext.getLogger(name);
    rootLogger.addAppender(logAppender);
  }
}
