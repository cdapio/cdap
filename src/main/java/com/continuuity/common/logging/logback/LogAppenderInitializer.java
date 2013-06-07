package com.continuuity.common.logging.logback;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.continuuity.common.conf.CConfiguration;
import com.google.inject.Inject;
import org.slf4j.LoggerFactory;

/**
 * Creates and sets the logback log appender.
 */
public class LogAppenderInitializer {
  @Inject
  public LogAppenderInitializer(CConfiguration configuration, AppenderBase<ILoggingEvent> logAppender) {
    LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
    logAppender.setContext(loggerContext);
    logAppender.start();

    Logger rootLogger = loggerContext.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    rootLogger.addAppender(logAppender);
  }
}
