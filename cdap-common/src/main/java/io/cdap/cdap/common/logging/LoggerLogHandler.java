/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.common.logging;

import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import javax.annotation.Nullable;

/**
 * A {@link LogHandler} implementation that logs {@link LogEntry} to {@link Logger}.
 */
public class LoggerLogHandler implements LogHandler {

  private static final Logger LOG = LoggerFactory.getLogger(LoggerLogHandler.class);
  private final LogHandler delegate;
  private final LogEntry.Level level;

  public LoggerLogHandler(Logger logger) {
    this(logger, null);
  }


  public LoggerLogHandler(Logger logger, @Nullable LogEntry.Level level) {
    this.level = level;

    if (logger instanceof ch.qos.logback.classic.Logger) {
      final ch.qos.logback.classic.Logger log = (ch.qos.logback.classic.Logger) logger;
      delegate = new LogHandler() {
        @Override
        public void onLog(LogEntry entry) {
          log.callAppenders(new TwillLogEntryAdapter(entry));
        }
      };
    } else {
      LOG.warn("Unsupported logger binding ({}) for container log collection. Falling back to System.out.",
               logger.getClass().getName());
      delegate = new PrinterLogHandler(new PrintWriter(System.out));
    }
  }

  @Override
  public void onLog(LogEntry logEntry) {
    if (level == null || level.ordinal() >= logEntry.getLogLevel().ordinal()) {
      delegate.onLog(logEntry);
    }
  }
}
