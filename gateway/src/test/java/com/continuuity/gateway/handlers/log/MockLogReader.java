package com.continuuity.gateway.handlers.log;

import com.continuuity.common.logging.LoggingContext;
import com.continuuity.logging.filter.Filter;
import com.continuuity.logging.read.Callback;
import com.continuuity.logging.read.LogEvent;
import com.continuuity.logging.read.LogReader;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.LoggingEvent;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* Mock LogReader for testing.
*/
public class MockLogReader implements LogReader {
  private static final Logger LOG = LoggerFactory.getLogger(MockLogReader.class);

  private final Multimap<String, LogLine> logMap;
  private static final int MAX = 80;

  MockLogReader() {
    logMap = ArrayListMultimap.create();

    // Add log lines for app testApp1, flow testFlow1
    for (int i = 0; i < MAX; ++i) {
      logMap.put(LogHandlerTest.account + "/testApp1/flow-testFlow1", new LogLine(i, "testFlow1<img>-" + i));
    }

    // Add log lines for app testApp1, flow testService1
    for (int i = 0; i < MAX; ++i) {
      logMap.put(LogHandlerTest.account + "/testApp4/userservice-testService1",
                 new LogLine(i, "testService1<img>-" + i));
    }

    // Add log lines for app testApp2, flow testProcedure1
    for (int i = 0; i < MAX; ++i) {
      logMap.put(LogHandlerTest.account + "/testApp2/procedure-testProcedure1",
                 new LogLine(i, "testProcedure1<img>-" + i));
    }

    // Add log lines for app testApp3, flow testMapReduce1
    for (int i = 0; i < MAX; ++i) {
      logMap.put(LogHandlerTest.account + "/testApp3/mapred-testMapReduce1",
                 new LogLine(i, "testMapReduce1<img>-" + i));
    }

  }

  @Override
  public void getLogNext(LoggingContext loggingContext, long fromOffset, int maxEvents, Filter filter,
                         Callback callback) {
    if (fromOffset < 0) {
      getLogPrev(loggingContext, fromOffset, maxEvents, filter, callback);
      return;
    }

    callback.init();
    try {
      int count = 0;
      for (LogLine logLine : logMap.get(loggingContext.getLogPathFragment())) {
        if (logLine.getOffset() >= fromOffset) {
          if (++count > maxEvents) {
            break;
          }

          if (filter != Filter.EMPTY_FILTER && logLine.getOffset() % 2 != 0) {
            continue;
          }

          callback.handle(
            new LogEvent(
              new LoggingEvent("com.continuiity.Test",
                               (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME),
                               Level.WARN, logLine.getLog(), null, null),
                                       logLine.getOffset())
          );
        }
      }
    } catch (Throwable e) {
      LOG.error("Got exception", e);
    } finally {
      callback.close();
    }
  }

  @Override
  public void getLogPrev(LoggingContext loggingContext, long fromOffset, int maxEvents, Filter filter,
                         Callback callback) {
    if (fromOffset < 0) {
      fromOffset = MAX;
    }

    callback.init();
    try {
      int count = 0;
      long startOffset = fromOffset - maxEvents;
      for (LogLine logLine : logMap.get(loggingContext.getLogPathFragment())) {
        if (logLine.getOffset() >= startOffset && logLine.getOffset() < fromOffset) {
          if (++count > maxEvents) {
            break;
          }

          if (filter != Filter.EMPTY_FILTER && logLine.getOffset() % 2 != 0) {
            continue;
          }

          callback.handle(
            new LogEvent(
              new LoggingEvent("com.continuiity.Test",
                               (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME),
                               Level.WARN, logLine.getLog(), null, null),
              logLine.getOffset())
          );
        }
      }
    } catch (Throwable e) {
      LOG.error("Got exception", e);
    } finally {
      callback.close();
    }
  }

  @Override
  public void getLog(LoggingContext loggingContext, long fromTimeMs, long toTimeMs, Filter filter,
                     Callback callback) {
    getLogNext(loggingContext, fromTimeMs / 1000, (int) (toTimeMs - fromTimeMs) / 1000, filter, callback);
  }

  @Override
  public void close() {
  }
}
