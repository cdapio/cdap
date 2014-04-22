package com.continuuity.logging.appender;

import com.continuuity.common.logging.LoggingContext;
import com.continuuity.logging.context.GenericLoggingContext;
import com.continuuity.logging.filter.Filter;
import com.continuuity.logging.read.Callback;
import com.continuuity.logging.read.LogEvent;
import com.continuuity.logging.read.LogReader;
import com.google.common.collect.Lists;
import org.junit.Assert;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 *
 */
public class LoggingTester {

  public void testGetNext(LogReader logReader, LoggingContext loggingContext) throws Exception {
    LogCallback logCallback1 = new LogCallback();
    logReader.getLogNext(loggingContext, -1, 10, Filter.EMPTY_FILTER, logCallback1);
    List<LogEvent> events = logCallback1.getEvents();
    Assert.assertEquals(10, events.size());
    Assert.assertEquals("Test log message 50 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 59 arg1 arg2", events.get(9).getLoggingEvent().getFormattedMessage());

    long ultimateOffset =  events.get(9).getOffset();
    long penultimateOffset = events.get(8).getOffset();

    loggingContext = new GenericLoggingContext("TFL_ACCT_1", "APP_1", "FLOW_1");
    LogCallback logCallback2 = new LogCallback();
    logReader.getLogPrev(loggingContext, logCallback1.getFirstOffset(), 20, Filter.EMPTY_FILTER,
                         logCallback2);
    events = logCallback2.getEvents();
    Assert.assertEquals(20, events.size());
    Assert.assertEquals("Test log message 30 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 49 arg1 arg2", events.get(19).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback3 = new LogCallback();
    logReader.getLogNext(loggingContext, logCallback2.getLastOffset(), 20, Filter.EMPTY_FILTER,
                         logCallback3);
    events = logCallback3.getEvents();
    Assert.assertEquals(10, events.size());
    Assert.assertEquals("Test log message 50 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 59 arg1 arg2", events.get(9).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback4 = new LogCallback();
    logReader.getLogNext(loggingContext, logCallback2.getFirstOffset(), 20, Filter.EMPTY_FILTER,
                         logCallback4);
    events = logCallback4.getEvents();
    Assert.assertEquals(20, events.size());
    Assert.assertEquals("Test log message 31 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 50 arg1 arg2", events.get(19).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback5 = new LogCallback();
    logReader.getLogNext(loggingContext, ultimateOffset, 20, Filter.EMPTY_FILTER,
                         logCallback5);
    events = logCallback5.getEvents();
    Assert.assertEquals(0, events.size());

    LogCallback logCallback6 = new LogCallback();
    logReader.getLogNext(loggingContext, ultimateOffset + 1, 20, Filter.EMPTY_FILTER,
                         logCallback6);
    events = logCallback6.getEvents();
    Assert.assertEquals(0, events.size());

    LogCallback logCallback7 = new LogCallback();
    logReader.getLogNext(loggingContext, penultimateOffset, 20, Filter.EMPTY_FILTER,
                         logCallback7);
    events = logCallback7.getEvents();
    Assert.assertEquals(1, events.size());
    Assert.assertEquals("Test log message 59 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
  }

  public void testGetPrev(LogReader logReader, LoggingContext loggingContext) throws Exception {
    LogCallback logCallback1 = new LogCallback();
    logReader.getLogPrev(loggingContext, -1, 10, Filter.EMPTY_FILTER, logCallback1);
    List<LogEvent> events = logCallback1.getEvents();
    Assert.assertEquals(10, events.size());
    Assert.assertEquals("Test log message 50 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 59 arg1 arg2", events.get(9).getLoggingEvent().getFormattedMessage());

    long ultimateOffset =  events.get(9).getOffset();

    loggingContext = new GenericLoggingContext("TFL_ACCT_1", "APP_1", "FLOW_1");
    LogCallback logCallback2 = new LogCallback();
    logReader.getLogPrev(loggingContext, logCallback1.getFirstOffset(), 20, Filter.EMPTY_FILTER,
                         logCallback2);
    events = logCallback2.getEvents();
    Assert.assertEquals(20, events.size());
    Assert.assertEquals("Test log message 30 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 49 arg1 arg2", events.get(19).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback3 = new LogCallback();
    logReader.getLogNext(loggingContext, logCallback2.getLastOffset(), 20, Filter.EMPTY_FILTER,
                         logCallback3);
    events = logCallback3.getEvents();
    Assert.assertEquals(10, events.size());
    Assert.assertEquals("Test log message 50 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 59 arg1 arg2", events.get(9).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback4 = new LogCallback();
    logReader.getLogPrev(loggingContext, logCallback2.getFirstOffset(), 15, Filter.EMPTY_FILTER,
                         logCallback4);
    events = logCallback4.getEvents();
    // In kafka mode, we'll get only 10 lines, need to run the call again.
    if (events.size() < 15) {
      LogCallback logCallback41 = new LogCallback();
      logReader.getLogPrev(loggingContext, logCallback4.getFirstOffset(), 5, Filter.EMPTY_FILTER,
                           logCallback41);
      events.addAll(0, logCallback41.getEvents());
      logCallback4 = logCallback41;
    }
    Assert.assertEquals(15, events.size());
    Assert.assertEquals("Test log message 15 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 29 arg1 arg2", events.get(14).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback5 = new LogCallback();
    logReader.getLogPrev(loggingContext, 0, 15, Filter.EMPTY_FILTER, logCallback5);
    events = logCallback5.getEvents();
    Assert.assertEquals(0, events.size());

    LogCallback logCallback6 = new LogCallback();
    logReader.getLogPrev(loggingContext, logCallback4.getFirstOffset(), 25, Filter.EMPTY_FILTER,
                         logCallback6);
    events = logCallback6.getEvents();
    Assert.assertEquals(15, events.size());
    Assert.assertEquals("Test log message 0 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 14 arg1 arg2", events.get(14).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback7 = new LogCallback();
    logReader.getLogPrev(loggingContext, logCallback4.getFirstOffset(), 15, Filter.EMPTY_FILTER,
                         logCallback7);
    events = logCallback7.getEvents();
    Assert.assertEquals(15, events.size());
    Assert.assertEquals("Test log message 0 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 14 arg1 arg2", events.get(14).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback9 = new LogCallback();
    logReader.getLogPrev(loggingContext, ultimateOffset + 1, 15, Filter.EMPTY_FILTER,
                         logCallback9);
    events = logCallback9.getEvents();
    Assert.assertEquals(15, events.size());
    Assert.assertEquals("Test log message 45 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 59 arg1 arg2", events.get(14).getLoggingEvent().getFormattedMessage());
  }

  /**
   * Log Call back for testing.
   */
  public static class LogCallback implements Callback {
    private long firstOffset;
    private long lastOffset;
    private List<LogEvent> events;
    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void init() {
      firstOffset = -1;
      lastOffset = -1;
      events = Lists.newArrayList();
    }

    @Override
    public void handle(LogEvent event) {
      if (firstOffset == -1) {
        firstOffset = event.getOffset();
      }
      lastOffset = event.getOffset();
      events.add(event);
    }

    @Override
    public void close() {
      latch.countDown();
    }

    public List<LogEvent> getEvents() throws Exception {
      latch.await();
      return events;
    }

    public long getFirstOffset() {
      return firstOffset;
    }

    public long getLastOffset() {
      return lastOffset;
    }
  }
}
