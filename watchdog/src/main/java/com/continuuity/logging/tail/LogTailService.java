package com.continuuity.logging.tail;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.LoggingConfiguration;
import com.continuuity.data.operation.executor.remote.ElasticPool;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 *
 */
public class LogTailService {
  private static final Logger LOG = LoggerFactory.getLogger(LogTailService.class);

  private final CConfiguration cConfig;
  private final Configuration hConfig;
  private final LogTailFactory logTailFactory;
  private final LogTailPool logTailPool;
  private final String logPattern;
  private static final int MAX_CLIENTS = 10;

  public LogTailService(CConfiguration cConfig, Configuration hConfig, LogTailFactory logTailFactory) {
    this.cConfig = cConfig;
    this.hConfig = hConfig;
    this.logTailFactory = logTailFactory;
    this.logTailPool = new LogTailPool(MAX_CLIENTS);
    this.logPattern = cConfig.get(LoggingConfiguration.LOG_PATTERN, "%d{ISO8601} - %-5p [%t:%C{1}@%L] - %m%n");
  }

  public List<String> getLog(String accountId, String applicationId, String flowId, long fromTimeMs, int maxEvents) {
    final List<ILoggingEvent> events = Lists.newArrayList();
    LogTail logTail = null;
    try {
      logTail = logTailPool.obtain();
      logTail.tailFlowLog(accountId, applicationId, flowId, fromTimeMs, maxEvents,
                          new Callback() {
                            @Override
                            public void handle(ILoggingEvent event) {
                              events.add(event);
                            }
                          });
      Collections.sort(events, LOGGING_EVENT_COMPARATOR);

      List<String> lines = Lists.newArrayListWithExpectedSize(events.size());
      ch.qos.logback.classic.Logger rootLogger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
      LoggerContext loggerContext = rootLogger.getLoggerContext();

      PatternLayout patternLayout = new PatternLayout();
      patternLayout.setContext(loggerContext);
      patternLayout.setPattern(logPattern);
      patternLayout.start();
      for (ILoggingEvent event : events) {
        lines.add(patternLayout.doLayout(event));
      }
      patternLayout.stop();
      return lines;
    } catch (Throwable e) {
      if (logTail != null) {
        logTailPool.discard(logTail);
      }
      throw Throwables.propagate(e);
    } finally {
      if (logTail != null) {
        logTailPool.release(logTail);
      }
    }
  }

  private static final Comparator<ILoggingEvent> LOGGING_EVENT_COMPARATOR =
    new Comparator<ILoggingEvent>() {
      @Override
      public int compare(ILoggingEvent e1, ILoggingEvent e2) {
        return e1.getTimeStamp() == e2.getTimeStamp() ? 0 : (e1.getTimeStamp() > e2.getTimeStamp() ? 1 : -1);
      }
    };

  private class LogTailPool extends ElasticPool<LogTail, Exception> {
    private LogTailPool(int sizeLimit) {
      super(sizeLimit);
    }

    @Override
    protected LogTail create() throws Exception {
      return logTailFactory.create(cConfig, hConfig);
    }

    @Override
    protected void destroy(LogTail client) {
      try {
        client.close();
      } catch (IOException e) {
        LOG.error("Exception when closing client", e);
      }
    }
  }
}
