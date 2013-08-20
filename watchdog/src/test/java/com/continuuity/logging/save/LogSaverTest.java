package com.continuuity.logging.save;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.logging.KafkaTestBase;
import com.continuuity.logging.LoggingConfiguration;
import com.continuuity.logging.Util;
import com.continuuity.logging.appender.LogAppenderInitializer;
import com.continuuity.logging.appender.kafka.KafkaLogAppender;
import com.continuuity.logging.appender.kafka.TestKafkaLogging;
import com.continuuity.logging.context.FlowletLoggingContext;
import com.continuuity.logging.filter.Filter;
import com.continuuity.logging.read.AvroFileLogReader;
import com.continuuity.logging.read.DistributedLogReader;
import com.continuuity.logging.read.LogEvent;
import com.continuuity.logging.serialize.LogSchema;
import com.google.common.collect.Maps;
import junit.framework.Assert;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import static com.continuuity.logging.appender.file.TestFileLogging.LogCallback;

/**
 * Test LogSaver and Distributed Log Reader.
 */
public class LogSaverTest extends KafkaTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(LogSaverTest.class);
  private static final OperationExecutor OPEX = Util.getOpex();

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void startLogSaver() throws Exception {
    String logBaseDir = temporaryFolder.newFolder().getAbsolutePath();
    LOG.info("Log base dir {}", logBaseDir);

    CConfiguration cConf = CConfiguration.create();
    cConf.set(LoggingConfiguration.KAFKA_SEED_BROKERS, "localhost:" + getKafkaPort());
    cConf.set(LoggingConfiguration.NUM_PARTITIONS, "1");
    cConf.set(LoggingConfiguration.LOG_BASE_DIR, logBaseDir);
    cConf.set(LoggingConfiguration.LOG_RUN_ACCOUNT, "developer");
    cConf.set(LoggingConfiguration.LOG_RETENTION_DURATION_DAYS, "10");
    cConf.set(LoggingConfiguration.LOG_MAX_FILE_SIZE_BYTES, "10240");
    cConf.set(LoggingConfiguration.LOG_FILE_SYNC_INTERVAL_BYTES, "5120");
    cConf.set(LoggingConfiguration.LOG_SAVER_EVENT_BUCKET_INTERVAL_MS, "300");
    cConf.set(LoggingConfiguration.LOG_SAVER_EVENT_PROCESSING_DELAY_MS, "600");

    LogSaver logSaver = new LogSaver(OPEX, 0, new Configuration(), cConf);
    logSaver.startAndWait();
    publishLogs();
    waitTillLogSaverDone(logBaseDir);
    logSaver.stopAndWait();
  }

  @Test
  public void testLogRead() throws Exception {
    CConfiguration conf = new CConfiguration();
    conf.set(LoggingConfiguration.KAFKA_SEED_BROKERS, "localhost:" + getKafkaPort());
    conf.set(LoggingConfiguration.NUM_PARTITIONS, "1");
    conf.set(LoggingConfiguration.LOG_RUN_ACCOUNT, "developer");
    DistributedLogReader distributedLogReader = new DistributedLogReader(OPEX, conf, new Configuration());

    LoggingContext loggingContext = new FlowletLoggingContext("ACCT_1", "APP_1", "FLOW_1", "");

    LogCallback logCallback1 = new LogCallback();
    distributedLogReader.getLog(loggingContext, 0, Long.MAX_VALUE, Filter.EMPTY_FILTER, logCallback1);
    List<LogEvent> allEvents = logCallback1.getEvents();

    Assert.assertEquals(60, allEvents.size());
    for (int i = 0; i < 60; ++i) {
      Assert.assertEquals(String.format("Test log message %d arg1 arg2", i),
                          allEvents.get(i).getLoggingEvent().getFormattedMessage());
    }

    LogCallback logCallback2 = new LogCallback();
    distributedLogReader.getLog(loggingContext, allEvents.get(10).getLoggingEvent().getTimeStamp(),
                                allEvents.get(20).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER, logCallback2);
    List<LogEvent> events = logCallback2.getEvents();
    Assert.assertEquals(10, events.size());
    Assert.assertEquals("Test log message 10 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 19 arg1 arg2", events.get(9).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback3 = new LogCallback();
    distributedLogReader.getLog(loggingContext, allEvents.get(5).getLoggingEvent().getTimeStamp(),
                                allEvents.get(55).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER, logCallback3);
    events = logCallback3.getEvents();
    Assert.assertEquals(50, events.size());
    Assert.assertEquals("Test log message 5 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 54 arg1 arg2", events.get(49).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback4 = new LogCallback();
    distributedLogReader.getLog(loggingContext, allEvents.get(30).getLoggingEvent().getTimeStamp(),
                                allEvents.get(53).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER, logCallback4);
    events = logCallback4.getEvents();
    Assert.assertEquals(23, events.size());
    Assert.assertEquals("Test log message 30 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 52 arg1 arg2", events.get(22).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback5 = new LogCallback();
    distributedLogReader.getLog(loggingContext, allEvents.get(35).getLoggingEvent().getTimeStamp(),
                                allEvents.get(38).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER, logCallback5);
    events = logCallback5.getEvents();
    Assert.assertEquals(3, events.size());
    Assert.assertEquals("Test log message 35 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 37 arg1 arg2", events.get(2).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback6 = new LogCallback();
    distributedLogReader.getLog(loggingContext, allEvents.get(53).getLoggingEvent().getTimeStamp(),
                                allEvents.get(59).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER, logCallback6);
    events = logCallback6.getEvents();
    Assert.assertEquals(6, events.size());
    Assert.assertEquals("Test log message 53 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 58 arg1 arg2", events.get(5).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback7 = new LogCallback();
    distributedLogReader.getLog(loggingContext, allEvents.get(59).getLoggingEvent().getTimeStamp(),
                                allEvents.get(59).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER, logCallback7);
    events = logCallback7.getEvents();
    Assert.assertEquals(0, events.size());

    LogCallback logCallback8 = new LogCallback();
    distributedLogReader.getLog(loggingContext, allEvents.get(0).getLoggingEvent().getTimeStamp(),
                                allEvents.get(0).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER, logCallback8);
    events = logCallback8.getEvents();
    Assert.assertEquals(0, events.size());

    LogCallback logCallback9 = new LogCallback();
    distributedLogReader.getLog(loggingContext, allEvents.get(20).getLoggingEvent().getTimeStamp(),
                                allEvents.get(20).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER, logCallback9);
    events = logCallback9.getEvents();
    Assert.assertEquals(0, events.size());

    LogCallback logCallback10 = new LogCallback();
    distributedLogReader.getLog(loggingContext, allEvents.get(32).getLoggingEvent().getTimeStamp() - 15,
                                allEvents.get(45).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER, logCallback10);
    events = logCallback10.getEvents();
    Assert.assertTrue(events.size() > 13);
    Assert.assertEquals("Test log message 32 arg1 arg2",
                        events.get(events.size() - 13).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 44 arg1 arg2",
                        events.get(events.size() - 1).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback11 = new LogCallback();
    distributedLogReader.getLog(loggingContext, allEvents.get(18).getLoggingEvent().getTimeStamp(),
                                allEvents.get(34).getLoggingEvent().getTimeStamp() + 18,
                                Filter.EMPTY_FILTER, logCallback11);
    events = logCallback11.getEvents();
    Assert.assertTrue(events.size() > 16);
    Assert.assertEquals("Test log message 18 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 33 arg1 arg2",
                        events.get(events.size() - 1 - (events.size() - 16)).getLoggingEvent().getFormattedMessage());
  }

  private static void publishLogs() throws Exception {
    LoggingContextAccessor.setLoggingContext(new FlowletLoggingContext("ACCT_1", "APP_1", "FLOW_1", "FLOWLET_1"));

    CConfiguration conf = CConfiguration.create();
    conf.set(LoggingConfiguration.KAFKA_SEED_BROKERS, "localhost:" + getKafkaPort());
    conf.set(LoggingConfiguration.NUM_PARTITIONS, "1");
    conf.set(LoggingConfiguration.KAFKA_PRODUCER_TYPE, "async");
    conf.set(LoggingConfiguration.KAFKA_PROCUDER_BUFFER_MS, "100");
    KafkaLogAppender appender = new KafkaLogAppender(conf);
    new LogAppenderInitializer(appender).initialize();

    Logger logger = LoggerFactory.getLogger(TestKafkaLogging.class);
    Exception e1 = new Exception("Test Exception1");
    Exception e2 = new Exception("Test Exception2", e1);

    for (int j = 0; j < 6; ++j) {
      for (int i = 0; i < 10; ++i) {
        logger.warn("Test log message " + (10 * j + i) + " {} {}", "arg1", "arg2", e2);
      }
      TimeUnit.MILLISECONDS.sleep(1200);
    }

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    StatusPrinter.setPrintStream(new PrintStream(bos));
    StatusPrinter.print((LoggerContext) LoggerFactory.getILoggerFactory());
    System.out.println(bos.toString());

    appender.stop();
  }

  private static void waitTillLogSaverDone(String logBaseDir) throws Exception {
    long start = System.currentTimeMillis();

    while (true) {
      String latestFile = getLatestFile(logBaseDir);
      if (latestFile != null) {
        AvroFileLogReader logReader = new AvroFileLogReader(new Configuration(), new LogSchema().getAvroSchema());
        LogCallback logCallback = new LogCallback();
        logCallback.init();
        logReader.readLog(new Path(latestFile), Filter.EMPTY_FILTER, 0, Long.MAX_VALUE, Integer.MAX_VALUE, logCallback);
        logCallback.close();
        List<LogEvent> events = logCallback.getEvents();
        if (events.size() > 0) {
          LogEvent event = events.get(events.size() - 1);
          if (event.getLoggingEvent().getFormattedMessage().equals("Test log message 59 arg1 arg2")) {
            break;
          }
        }
      }

      Assert.assertTrue("Time exceeded", System.currentTimeMillis() - start < 30 * 1000);
      TimeUnit.SECONDS.sleep(1);
    }

    LOG.info("Done waiting!");
    TimeUnit.SECONDS.sleep(1);
  }

  private static String getLatestFile(String logBaseDir) throws Exception {
    String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    File dir = new File(logBaseDir, String.format("ACCT_1/APP_1/flow-FLOW_1/%s", date));
    File[] files = FileUtil.listFiles(dir);
    if (files.length == 0) {
      return null;
    }

    SortedMap<Long, String> map = Maps.newTreeMap();
    for (File file : files) {
      String filename = FilenameUtils.getBaseName(file.getAbsolutePath());
      map.put(Long.parseLong(filename), file.getAbsolutePath());
    }
    return map.get(map.lastKey());
  }
}
