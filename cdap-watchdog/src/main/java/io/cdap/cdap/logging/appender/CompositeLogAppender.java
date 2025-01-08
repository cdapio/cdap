package io.cdap.cdap.logging.appender;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Context;
import io.cdap.cdap.logging.framework.local.LocalLogAppender;
import java.util.List;
import org.slf4j.LoggerFactory;

public class CompositeLogAppender extends LogAppender {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(CompositeLogAppender.class);
  private final List<LogAppender> appenders;

  public CompositeLogAppender(List<LogAppender> appenders) {
    this.appenders = appenders;
    setName(getClass().getName());
  }

  @Override
  public void start() {
    appenders.forEach(appender -> safelyExecute(
        appender::start,
        "Failed to start appender: " + appender.getName()
    ));
    super.start();
  }

  @Override
  public void stop() {
    super.stop();
    appenders.forEach(appender -> safelyExecute(
        appender::stop,
        "Failed to stop appender: " + appender.getName()
    ));
  }

  @Override
  protected void appendEvent(LogMessage logMessage) {
    appenders.forEach(appender -> safelyExecute(
        () -> appender.appendEvent(logMessage),
        "Failed to append log message to appender: " + appender.getName() + ". Message: "
            + logMessage.getFormattedMessage()
    ));
  }

  @Override
  public void setContext(Context context) {
    super.setContext(context);
    appenders.forEach(appender -> safelyExecute(
        () -> appender.setContext(context),
        "Failed to set context to appender: " + appender.getName()
    ));
  }

  @Override
  public void doAppend(ILoggingEvent eventObject) {
    if (shouldSkipLogging()) {
      return;
    }
    super.doAppend(eventObject);
  }

  boolean shouldSkipLogging() {
    return appenders.stream()
        .filter(appender -> appender instanceof LocalLogAppender)
        .map(appender -> (LocalLogAppender) appender)
        .anyMatch(localAppender -> localAppender.getPipelineThreads()
            .get()
            .contains(Thread.currentThread()));
  }

  private void safelyExecute(Runnable action, String errorMessage) {
    try {
      action.run();
    } catch (Exception e) {
      LOG.warn(errorMessage, e);
    }
  }
}
