/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.log;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Sets up a LogAppender that will inject the ETL stage name into the log message.
 */
public class LogStageInjector {
  private static final Logger LOG = LoggerFactory.getLogger(LogStageInjector.class);
  private static final AtomicBoolean initialized = new AtomicBoolean(false);

  private LogStageInjector() {

  }

  /**
   * Hijacks the appenders for the root logger and replaces them with a {@link LogStageAppender} that will insert
   * the ETL stage name at the start of each message if the stage name is set. Uses {@link org.slf4j.MDC} to look up
   * the current stage name.
   */
  public static void start() {
    if (!initialized.compareAndSet(false, true)) {
      return;
    }

    ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
    if (!(loggerFactory instanceof LoggerContext)) {
      LOG.warn("LoggerFactory is not a logback LoggerContext. Stage names will not be injected into log messages.");
      return;
    }

    LoggerContext loggerContext = (LoggerContext) loggerFactory;
    ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);

    List<Appender<ILoggingEvent>> appenders = new ArrayList<>();
    Iterator<Appender<ILoggingEvent>> appenderIterator = rootLogger.iteratorForAppenders();
    while (appenderIterator.hasNext()) {
      Appender<ILoggingEvent> appender = appenderIterator.next();
      // this is only for cdap standalone, where the same jvm launches multiple mapreduce jobs
      // in those scenarios, the second time a program runs and gets to this part of the code,
      // the root logger will already have the LogStageAppender as its appender. In that case, just return immediately.
      // hack... can't do (appender instanceof LogStageAppender) here because
      // appender will have a different classloader than LogStageAppender...
      if (appender.getClass().getName().equals(LogStageAppender.class.getName())) {
        return;
      }
      appenders.add(appender);
    }
    Appender<ILoggingEvent> stageAppender = new LogStageAppender(appenders);
    stageAppender.setContext(loggerContext);
    stageAppender.start();

    rootLogger.addAppender(stageAppender);
    // the LogStageAppender calls the original appenders.
    // To avoid duplicate messages, need to detach the original appenders
    for (Appender<ILoggingEvent> appender : appenders) {
      rootLogger.detachAppender(appender);
    }
  }

}
