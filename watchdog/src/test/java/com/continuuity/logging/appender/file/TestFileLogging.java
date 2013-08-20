package com.continuuity.logging.appender.file;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.logging.LoggingConfiguration;
import com.continuuity.logging.appender.LogAppenderInitializer;
import com.continuuity.logging.context.FlowletLoggingContext;
import com.continuuity.logging.filter.Filter;
import com.continuuity.logging.read.Callback;
import com.continuuity.logging.read.LogEvent;
import com.continuuity.logging.read.SingleNodeLogReader;
import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Test logging to Avro file.
 */
public class TestFileLogging {
  private static String LOG_BASE_DIR;

  @BeforeClass
  public static void setUpContext() throws Exception {
    LoggingContextAccessor.setLoggingContext(new FlowletLoggingContext("ACCT_1", "APP_1", "FLOW_1", "FLOWLET_1"));
    LOG_BASE_DIR = "file:///tmp/log_files_" + new Random(System.currentTimeMillis()).nextLong();


    CConfiguration cConf = CConfiguration.create();
    cConf.set(LoggingConfiguration.LOG_BASE_DIR, LOG_BASE_DIR);
    cConf.set(LoggingConfiguration.LOG_FILE_ROTATION_INTERVAL_MINS, "0.015");
    cConf.setInt(LoggingConfiguration.LOG_FILE_SYNC_INTERVAL_BYTES, 100);
    FileLogAppender appender = new FileLogAppender(cConf, new Configuration());
    new LogAppenderInitializer(appender).initialize();

    Logger logger = LoggerFactory.getLogger(TestFileLogging.class);
    for (int i = 0; i < 20; ++i) {
      Exception e1 = new Exception("Test Exception1");
      Exception e2 = new Exception("Test Exception2", e1);
      logger.warn("Test log message " + i + " {} {}", "arg1", "arg2", e2);
    }

    TimeUnit.SECONDS.sleep(1);
    appender.getLogFileWriter().rotate(System.currentTimeMillis());

    for (int i = 20; i < 40; ++i) {
      Exception e1 = new Exception("Test Exception1");
      Exception e2 = new Exception("Test Exception2", e1);
      logger.warn("Test log message " + i + " {} {}", "arg1", "arg2", e2);
    }

    TimeUnit.SECONDS.sleep(1);
    appender.getLogFileWriter().rotate(System.currentTimeMillis());

    for (int i = 40; i < 60; ++i) {
      Exception e1 = new Exception("Test Exception1");
      Exception e2 = new Exception("Test Exception2", e1);
      logger.warn("Test log message " + i + " {} {}", "arg1", "arg2", e2);
    }

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    StatusPrinter.setPrintStream(new PrintStream(bos));
    StatusPrinter.print((LoggerContext) LoggerFactory.getILoggerFactory());
    System.out.println(bos.toString());
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    FileUtils.deleteDirectory(new File(new Path(LOG_BASE_DIR).toUri()));
  }

  @Test
  public void testGetLogNext() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(LoggingConfiguration.LOG_BASE_DIR, LOG_BASE_DIR);

    LoggingContext loggingContext = new FlowletLoggingContext("ACCT_1", "APP_1", "FLOW_1", "");
    SingleNodeLogReader logTail = new SingleNodeLogReader(cConf, new Configuration());
    LogCallback logCallback1 = new LogCallback();
    logTail.getLogNext(loggingContext, -1, 20, Filter.EMPTY_FILTER,
                       logCallback1);
    List<LogEvent> events = logCallback1.getEvents();
    Assert.assertEquals(20, events.size());
    Assert.assertEquals("Test log message 40 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 59 arg1 arg2", events.get(19).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback2 = new LogCallback();
    logTail.getLogPrev(loggingContext, logCallback1.getFirstOffset(), 15, Filter.EMPTY_FILTER,
                       logCallback2);
    events = logCallback2.getEvents();
    Assert.assertEquals(15, events.size());
    Assert.assertEquals("Test log message 25 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 39 arg1 arg2", events.get(14).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback3 = new LogCallback();
    logTail.getLogNext(loggingContext, logCallback2.getFirstOffset(), 10, Filter.EMPTY_FILTER,
                       logCallback3);
    events = logCallback3.getEvents();
    Assert.assertEquals(10, events.size());
    Assert.assertEquals("Test log message 26 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 35 arg1 arg2", events.get(9).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback4 = new LogCallback();
    logTail.getLogNext(loggingContext, logCallback3.getLastOffset(), 10, Filter.EMPTY_FILTER,
                       logCallback4);
    events = logCallback4.getEvents();
    Assert.assertEquals(10, events.size());
    Assert.assertEquals("Test log message 36 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 45 arg1 arg2", events.get(9).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback5 = new LogCallback();
    logTail.getLogNext(loggingContext, logCallback4.getLastOffset(), 20, Filter.EMPTY_FILTER,
                       logCallback5);
    events = logCallback5.getEvents();
    Assert.assertEquals(14, events.size());
    Assert.assertEquals("Test log message 46 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 59 arg1 arg2", events.get(13).getLoggingEvent().getFormattedMessage());
    logTail.close();
  }

  @Test
  public void testGetLogPrev() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(LoggingConfiguration.LOG_BASE_DIR, LOG_BASE_DIR);

    LoggingContext loggingContext = new FlowletLoggingContext("ACCT_1", "APP_1", "FLOW_1", "");
    SingleNodeLogReader logTail = new SingleNodeLogReader(cConf, new Configuration());
    LogCallback logCallback1 = new LogCallback();
    logTail.getLogPrev(loggingContext, -1, 20, Filter.EMPTY_FILTER,
                       logCallback1);
    List<LogEvent> events = logCallback1.getEvents();
    Assert.assertEquals(20, events.size());
    Assert.assertEquals("Test log message 40 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 59 arg1 arg2", events.get(19).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback2 = new LogCallback();
    logTail.getLogPrev(loggingContext, logCallback1.getFirstOffset(), 15, Filter.EMPTY_FILTER,
                       logCallback2);
    events = logCallback2.getEvents();
    Assert.assertEquals(15, events.size());
    Assert.assertEquals("Test log message 25 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 39 arg1 arg2", events.get(14).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback3 = new LogCallback();
    logTail.getLogPrev(loggingContext, logCallback2.getFirstOffset(), 10, Filter.EMPTY_FILTER,
                       logCallback3);
    events = logCallback3.getEvents();
    Assert.assertEquals(10, events.size());
    Assert.assertEquals("Test log message 15 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 24 arg1 arg2", events.get(9).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback4 = new LogCallback();
    logTail.getLogNext(loggingContext, logCallback3.getLastOffset(), 21, Filter.EMPTY_FILTER,
                       logCallback4);
    events = logCallback4.getEvents();
    Assert.assertEquals(21, events.size());
    Assert.assertEquals("Test log message 25 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 45 arg1 arg2", events.get(20).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback5 = new LogCallback();
    logTail.getLogNext(loggingContext, logCallback4.getLastOffset(), 20, Filter.EMPTY_FILTER,
                       logCallback5);
    events = logCallback5.getEvents();
    Assert.assertEquals(14, events.size());
    Assert.assertEquals("Test log message 46 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 59 arg1 arg2", events.get(13).getLoggingEvent().getFormattedMessage());
    logTail.close();
  }

  @Test
  public void testGetLog() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(LoggingConfiguration.LOG_BASE_DIR, LOG_BASE_DIR);

    LoggingContext loggingContext = new FlowletLoggingContext("ACCT_1", "APP_1", "FLOW_1", "");
    SingleNodeLogReader logTail = new SingleNodeLogReader(cConf, new Configuration());
    LogCallback logCallback1 = new LogCallback();
    logTail.getLogPrev(loggingContext, -1, 60, Filter.EMPTY_FILTER,
                       logCallback1);
    List<LogEvent> allEvents = logCallback1.getEvents();
    Assert.assertEquals(60, allEvents.size());

    LogCallback logCallback2 = new LogCallback();
    logTail.getLog(loggingContext, allEvents.get(10).getLoggingEvent().getTimeStamp(),
                   allEvents.get(15).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER, logCallback2);
    List<LogEvent> events = logCallback2.getEvents();
    Assert.assertEquals(5, events.size());
    Assert.assertEquals(allEvents.get(10).getLoggingEvent().getFormattedMessage(),
                        events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals(allEvents.get(14).getLoggingEvent().getFormattedMessage(),
                        events.get(4).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback3 = new LogCallback();
    logTail.getLog(loggingContext, allEvents.get(0).getLoggingEvent().getTimeStamp(),
                   allEvents.get(59).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER, logCallback3);
    events = logCallback3.getEvents();
    Assert.assertEquals(59, events.size());
    Assert.assertEquals(allEvents.get(0).getLoggingEvent().getFormattedMessage(),
                        events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals(allEvents.get(58).getLoggingEvent().getFormattedMessage(),
                        events.get(58).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback4 = new LogCallback();
    logTail.getLog(loggingContext, allEvents.get(12).getLoggingEvent().getTimeStamp(),
                   allEvents.get(41).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER, logCallback4);
    events = logCallback4.getEvents();
    Assert.assertEquals(29, events.size());
    Assert.assertEquals(allEvents.get(12).getLoggingEvent().getFormattedMessage(),
                        events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals(allEvents.get(40).getLoggingEvent().getFormattedMessage(),
                        events.get(28).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback5 = new LogCallback();
    logTail.getLog(loggingContext, allEvents.get(22).getLoggingEvent().getTimeStamp(),
                   allEvents.get(38).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER, logCallback5);
    events = logCallback5.getEvents();
    Assert.assertEquals(16, events.size());
    Assert.assertEquals(allEvents.get(22).getLoggingEvent().getFormattedMessage(),
                        events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals(allEvents.get(37).getLoggingEvent().getFormattedMessage(),
                        events.get(15).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback6 = new LogCallback();
    logTail.getLog(loggingContext, allEvents.get(41).getLoggingEvent().getTimeStamp(),
                   allEvents.get(59).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER, logCallback6);
    events = logCallback6.getEvents();
    Assert.assertEquals(18, events.size());
    Assert.assertEquals(allEvents.get(41).getLoggingEvent().getFormattedMessage(),
                        events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals(allEvents.get(58).getLoggingEvent().getFormattedMessage(),
                        events.get(17).getLoggingEvent().getFormattedMessage());

  }

  /**
   * Log Call back for TestFileLogging.
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

    public List<LogEvent> getEvents() throws Exception{
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
