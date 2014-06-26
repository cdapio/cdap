package com.continuuity.logging.appender;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.status.OnConsoleStatusListener;
import ch.qos.logback.core.status.StatusManager;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.ConcurrentMap;

/**
 * Creates and sets the logback log appender.
 */
public class LogAppenderInitializer implements Closeable {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LogAppenderInitializer.class);
  private static final ConcurrentMap<String, String> initMap = Maps.newConcurrentMap();
  private final LogAppender logAppender;

  @Inject
  public LogAppenderInitializer(LogAppender logAppender) {
    this.logAppender = logAppender;
  }

  public LogAppender initialize() {
    return initialize(org.slf4j.Logger.ROOT_LOGGER_NAME);
  }

  public LogAppender initialize(String rootLoggerName) {
    ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
    // TODO: fix logging issue in mapreduce:  ENG-3279
    if (!(loggerFactory instanceof LoggerContext)) {
      LOG.warn("LoggerFactory is not a logback LoggerContext. No log appender is added. " +
                 "Logback might not be in the classpath");
      return null;
    }

    LoggerContext loggerContext = (LoggerContext) loggerFactory;
    Logger rootLogger = loggerContext.getLogger(rootLoggerName);

    if (initMap.putIfAbsent(rootLoggerName, logAppender.getName()) != null) {
      // Already initialized.
      LOG.trace("Log appender {} is already initialized.", logAppender.getName());

      Appender<ILoggingEvent> appender = rootLogger.getAppender(logAppender.getName());
      if (appender instanceof LogAppender) {
        return (LogAppender) appender;
      } else {
        throw new IllegalArgumentException(
          String.format("%s appender is already initialized, but not able to fetch it",
                        logAppender.getName()));
      }
    }

    LOG.info("Initializing log appender {}", logAppender.getName());


    // Display any errors during initialization of log appender to console
    StatusManager statusManager = loggerContext.getStatusManager();
    OnConsoleStatusListener onConsoleListener = new OnConsoleStatusListener();
    statusManager.add(onConsoleListener);

    logAppender.setContext(loggerContext);
    logAppender.start();

    rootLogger.addAppender(logAppender);
    return logAppender;
  }

  @Override
  public void close() {
    if (logAppender != null) {
      LOG.info("Stopping log appender {}", logAppender.getName());
      logAppender.stop();
    }
  }
}
