package com.continuuity.logging.appender.file;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.data.InMemoryDataSetAccessor;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.continuuity.logging.LoggingConfiguration;
import com.continuuity.logging.appender.LogAppenderInitializer;
import com.continuuity.logging.appender.LoggingTester;
import com.continuuity.logging.context.FlowletLoggingContext;
import com.continuuity.logging.filter.Filter;
import com.continuuity.logging.read.LogEvent;
import com.continuuity.logging.read.SingleNodeLogReader;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.List;
import java.util.Random;

/**
 * Test logging to Avro file.
 */
public class TestFileLogging {
  private static String logBaseDir;

  private static InMemoryDataSetAccessor dataSetAccessor = new InMemoryDataSetAccessor(CConfiguration.create());
  private static InMemoryTxSystemClient txClient;

  @BeforeClass
  public static void setUpContext() throws Exception {
    LoggingContextAccessor.setLoggingContext(new FlowletLoggingContext("TFL_ACCT_1", "APP_1", "FLOW_1", "FLOWLET_1"));
    logBaseDir = "/tmp/log_files_" + new Random(System.currentTimeMillis()).nextLong();


    CConfiguration cConf = CConfiguration.create();
    cConf.set(LoggingConfiguration.LOG_BASE_DIR, logBaseDir);
    cConf.setInt(LoggingConfiguration.LOG_MAX_FILE_SIZE_BYTES, 20 * 1024);

    InMemoryTransactionManager txManager = new InMemoryTransactionManager();
    txManager.startAndWait();
    txClient = new InMemoryTxSystemClient(txManager);

    FileLogAppender appender = new FileLogAppender(cConf, dataSetAccessor, txClient, new LocalLocationFactory());
    new LogAppenderInitializer(appender).initialize("TestFileLogging");

    Logger logger = LoggerFactory.getLogger("TestFileLogging");
    for (int i = 0; i < 60; ++i) {
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
    FileUtils.deleteDirectory(new File(logBaseDir));
  }

  @Test
  public void testGetLogNext() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(LoggingConfiguration.LOG_BASE_DIR, logBaseDir);

    LoggingContext loggingContext = new FlowletLoggingContext("TFL_ACCT_1", "APP_1", "FLOW_1", "");
    SingleNodeLogReader logReader =
      new SingleNodeLogReader(cConf, dataSetAccessor, txClient, new LocalLocationFactory());
    LoggingTester tester = new LoggingTester();
    tester.testGetNext(logReader, loggingContext);
    logReader.close();
  }

  @Test
  public void testGetLogPrev() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(LoggingConfiguration.LOG_BASE_DIR, logBaseDir);

    LoggingContext loggingContext = new FlowletLoggingContext("TFL_ACCT_1", "APP_1", "FLOW_1", "");
    SingleNodeLogReader logReader =
      new SingleNodeLogReader(cConf, dataSetAccessor, txClient, new LocalLocationFactory());
    LoggingTester tester = new LoggingTester();
    tester.testGetPrev(logReader, loggingContext);
    logReader.close();
  }

  @Test
  public void testGetLog() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(LoggingConfiguration.LOG_BASE_DIR, logBaseDir);

    LoggingContext loggingContext = new FlowletLoggingContext("TFL_ACCT_1", "APP_1", "FLOW_1", "");
    SingleNodeLogReader logTail =
      new SingleNodeLogReader(cConf, dataSetAccessor, txClient, new LocalLocationFactory());
    LoggingTester.LogCallback logCallback1 = new LoggingTester.LogCallback();
    logTail.getLogPrev(loggingContext, -1, 60, Filter.EMPTY_FILTER,
                       logCallback1);
    List<LogEvent> allEvents = logCallback1.getEvents();
    Assert.assertEquals(60, allEvents.size());

    LoggingTester.LogCallback logCallback2 = new LoggingTester.LogCallback();
    logTail.getLog(loggingContext, allEvents.get(10).getLoggingEvent().getTimeStamp(),
                   allEvents.get(15).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER, logCallback2);
    List<LogEvent> events = logCallback2.getEvents();
    Assert.assertEquals(5, events.size());
    Assert.assertEquals(allEvents.get(10).getLoggingEvent().getFormattedMessage(),
                        events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals(allEvents.get(14).getLoggingEvent().getFormattedMessage(),
                        events.get(4).getLoggingEvent().getFormattedMessage());

    LoggingTester.LogCallback logCallback3 = new LoggingTester.LogCallback();
    logTail.getLog(loggingContext, allEvents.get(0).getLoggingEvent().getTimeStamp(),
                   allEvents.get(59).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER, logCallback3);
    events = logCallback3.getEvents();
    Assert.assertEquals(59, events.size());
    Assert.assertEquals(allEvents.get(0).getLoggingEvent().getFormattedMessage(),
                        events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals(allEvents.get(58).getLoggingEvent().getFormattedMessage(),
                        events.get(58).getLoggingEvent().getFormattedMessage());

    LoggingTester.LogCallback logCallback4 = new LoggingTester.LogCallback();
    logTail.getLog(loggingContext, allEvents.get(12).getLoggingEvent().getTimeStamp(),
                   allEvents.get(41).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER, logCallback4);
    events = logCallback4.getEvents();
    Assert.assertEquals(29, events.size());
    Assert.assertEquals(allEvents.get(12).getLoggingEvent().getFormattedMessage(),
                        events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals(allEvents.get(40).getLoggingEvent().getFormattedMessage(),
                        events.get(28).getLoggingEvent().getFormattedMessage());

    LoggingTester.LogCallback logCallback5 = new LoggingTester.LogCallback();
    logTail.getLog(loggingContext, allEvents.get(22).getLoggingEvent().getTimeStamp(),
                   allEvents.get(38).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER, logCallback5);
    events = logCallback5.getEvents();
    Assert.assertEquals(16, events.size());
    Assert.assertEquals(allEvents.get(22).getLoggingEvent().getFormattedMessage(),
                        events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals(allEvents.get(37).getLoggingEvent().getFormattedMessage(),
                        events.get(15).getLoggingEvent().getFormattedMessage());

    LoggingTester.LogCallback logCallback6 = new LoggingTester.LogCallback();
    logTail.getLog(loggingContext, allEvents.get(41).getLoggingEvent().getTimeStamp(),
                   allEvents.get(59).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER, logCallback6);
    events = logCallback6.getEvents();
    Assert.assertEquals(18, events.size());
    Assert.assertEquals(allEvents.get(41).getLoggingEvent().getFormattedMessage(),
                        events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals(allEvents.get(58).getLoggingEvent().getFormattedMessage(),
                        events.get(17).getLoggingEvent().getFormattedMessage());

  }

}
