package com.continuuity.logging.save;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.AccountLoggingContext;
import com.continuuity.common.logging.ApplicationLoggingContext;
import com.continuuity.common.logging.ComponentLoggingContext;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.common.logging.ServiceLoggingContext;
import com.continuuity.common.logging.SystemLoggingContext;
import com.continuuity.data.InMemoryDataSetAccessor;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.continuuity.logging.KafkaTestBase;
import com.continuuity.logging.LoggingConfiguration;
import com.continuuity.logging.appender.LogAppenderInitializer;
import com.continuuity.logging.appender.kafka.KafkaLogAppender;
import com.continuuity.logging.appender.kafka.KafkaTopic;
import com.continuuity.logging.context.FlowletLoggingContext;
import com.continuuity.logging.filter.Filter;
import com.continuuity.logging.read.AvroFileLogReader;
import com.continuuity.logging.read.DistributedLogReader;
import com.continuuity.logging.read.LogEvent;
import com.continuuity.logging.serialize.LogSchema;
import com.continuuity.test.SlowTests;
import com.continuuity.watchdog.election.MultiLeaderElection;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.continuuity.logging.appender.LoggingTester.LogCallback;

/**
 * Test LogSaver and Distributed Log Reader.
 */
@Category(SlowTests.class)
public class LogSaverTest extends KafkaTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(LogSaverTest.class);

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static InMemoryTxSystemClient txClient = null;
  private static InMemoryDataSetAccessor dataSetAccessor = new InMemoryDataSetAccessor(CConfiguration.create());
  private static LogSaverTableUtil tableUtil;

  @BeforeClass
  public static void startLogSaver() throws Exception {
    String logBaseDir = temporaryFolder.newFolder().getAbsolutePath();
    LOG.info("Log base dir {}", logBaseDir);

    CConfiguration cConf = CConfiguration.create();
    cConf.set(LoggingConfiguration.KAFKA_SEED_BROKERS, "localhost:" + getKafkaPort());
    cConf.set(LoggingConfiguration.NUM_PARTITIONS, "2");
    cConf.set(LoggingConfiguration.LOG_BASE_DIR, logBaseDir);
    cConf.set(LoggingConfiguration.LOG_RUN_ACCOUNT, "developer");
    cConf.set(LoggingConfiguration.LOG_RETENTION_DURATION_DAYS, "10");
    cConf.set(LoggingConfiguration.LOG_MAX_FILE_SIZE_BYTES, "10240");
    cConf.set(LoggingConfiguration.LOG_FILE_SYNC_INTERVAL_BYTES, "5120");
    cConf.set(LoggingConfiguration.LOG_SAVER_EVENT_BUCKET_INTERVAL_MS, "300");
    cConf.set(LoggingConfiguration.LOG_SAVER_EVENT_PROCESSING_DELAY_MS, "600");
    cConf.set(LoggingConfiguration.LOG_SAVER_TOPIC_WAIT_SLEEP_MS, "10");

    InMemoryTransactionManager txManager = new InMemoryTransactionManager();
    txManager.startAndWait();
    txClient = new InMemoryTxSystemClient(txManager);

    ZKClientService zkClientService = ZKClientServices.delegate(
      ZKClients.reWatchOnExpire(
        ZKClients.retryOnFailure(
          ZKClientService.Builder.of(getZkConnectString()).build(),
          RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS)
        )
      ));
    zkClientService.startAndWait();

    KafkaClientService kafkaClient = new ZKKafkaClientService(zkClientService);
    kafkaClient.startAndWait();

    tableUtil = new LogSaverTableUtil(dataSetAccessor);
    LogSaver logSaver =
      new LogSaver(tableUtil, txClient, kafkaClient,
                   cConf, new LocalLocationFactory());

    logSaver.startAndWait();

    MultiLeaderElection multiElection = new MultiLeaderElection(zkClientService, "log-saver", 2, logSaver);
    multiElection.setLeaderElectionSleepMs(1);
    multiElection.startAndWait();

    // Sleep a while to let Kafka server fully initialized.
    TimeUnit.SECONDS.sleep(5);

    publishLogs();

    waitTillLogSaverDone(logBaseDir, "ACCT_1/APP_1/flow-FLOW_1/%s", "Test log message 59 arg1 arg2");
    waitTillLogSaverDone(logBaseDir, "ACCT_2/APP_2/flow-FLOW_2/%s", "Test log message 59 arg1 arg2");
    waitTillLogSaverDone(logBaseDir, "reactor/services/service-metrics/%s", "Test log message 59 arg1 arg2");

    logSaver.stopAndWait();
    multiElection.stopAndWait();
    kafkaClient.stopAndWait();
    zkClientService.stopAndWait();
  }

  @AfterClass
  public static void testCheckpoint() throws Exception {
    CheckpointManager checkpointManager = new CheckpointManager(tableUtil.getMetaTable(),
                                                                txClient, KafkaTopic.getTopic());
    Assert.assertEquals(60, checkpointManager.getCheckpoint(0));
    Assert.assertEquals(120, checkpointManager.getCheckpoint(1));
  }

  @Test
  public void testLogRead2() throws Exception {
    testLogRead(new FlowletLoggingContext("ACCT_1", "APP_1", "FLOW_1", ""));
  }

  @Test
  public void testLogRead1() throws Exception {
    testLogRead(new FlowletLoggingContext("ACCT_2", "APP_2", "FLOW_2", ""));
  }

  @Test
  public void testLogRead3() throws Exception {
    testLogRead(new ServiceLoggingContext("reactor", "services", "metrics"));
  }

  private void testLogRead(LoggingContext loggingContext) throws Exception {
    LOG.info("Verifying logging context {}", loggingContext.getLogPathFragment());
    CConfiguration conf = new CConfiguration();
    conf.set(LoggingConfiguration.KAFKA_SEED_BROKERS, "localhost:" + getKafkaPort());
    conf.set(LoggingConfiguration.NUM_PARTITIONS, "2");
    conf.set(LoggingConfiguration.LOG_RUN_ACCOUNT, "developer");
    DistributedLogReader distributedLogReader =
      new DistributedLogReader(new InMemoryDataSetAccessor(conf), txClient, conf, new LocalLocationFactory());

    LogCallback logCallback1 = new LogCallback();
    distributedLogReader.getLog(loggingContext, 0, Long.MAX_VALUE, Filter.EMPTY_FILTER, logCallback1);
    List<LogEvent> allEvents = logCallback1.getEvents();

    Assert.assertEquals(60, allEvents.size());
    for (int i = 0; i < 60; ++i) {
      Assert.assertEquals(String.format("Test log message %d arg1 arg2", i),
                          allEvents.get(i).getLoggingEvent().getFormattedMessage());
      if (loggingContext instanceof ServiceLoggingContext) {
        Assert.assertEquals(
          loggingContext.getSystemTagsMap().get(SystemLoggingContext.TAG_SYSTEM_ID).getValue(),
          allEvents.get(i).getLoggingEvent().getMDCPropertyMap().get(SystemLoggingContext.TAG_SYSTEM_ID));
        Assert.assertEquals(
          loggingContext.getSystemTagsMap().get(ComponentLoggingContext.TAG_COMPONENT_ID).getValue(),
          allEvents.get(i).getLoggingEvent().getMDCPropertyMap().get(ComponentLoggingContext.TAG_COMPONENT_ID));
        Assert.assertEquals(
          loggingContext.getSystemTagsMap().get(ServiceLoggingContext.TAG_SERVICE_ID).getValue(),
          allEvents.get(i).getLoggingEvent().getMDCPropertyMap().get(ServiceLoggingContext.TAG_SERVICE_ID));
      } else {
        Assert.assertEquals(
          loggingContext.getSystemTagsMap().get(AccountLoggingContext.TAG_ACCOUNT_ID).getValue(),
          allEvents.get(i).getLoggingEvent().getMDCPropertyMap().get(AccountLoggingContext.TAG_ACCOUNT_ID));
        Assert.assertEquals(
          loggingContext.getSystemTagsMap().get(ApplicationLoggingContext.TAG_APPLICATION_ID).getValue(),
          allEvents.get(i).getLoggingEvent().getMDCPropertyMap().get(ApplicationLoggingContext.TAG_APPLICATION_ID));
        Assert.assertEquals(
          loggingContext.getSystemTagsMap().get(FlowletLoggingContext.TAG_FLOW_ID).getValue(),
          allEvents.get(i).getLoggingEvent().getMDCPropertyMap().get(FlowletLoggingContext.TAG_FLOW_ID));
      }
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
    distributedLogReader.getLog(loggingContext, allEvents.get(32).getLoggingEvent().getTimeStamp() - 999999,
                                allEvents.get(45).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER, logCallback10);
    events = logCallback10.getEvents();
    Assert.assertTrue(events.size() > 13);
    Assert.assertEquals("Test log message 32 arg1 arg2",
                        events.get(events.size() - 13).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 44 arg1 arg2",
                        events.get(events.size() - 1).getLoggingEvent().getFormattedMessage());

    LogCallback logCallback11 = new LogCallback();
    distributedLogReader.getLog(loggingContext, allEvents.get(18).getLoggingEvent().getTimeStamp(),
                                allEvents.get(34).getLoggingEvent().getTimeStamp() + 999999,
                                Filter.EMPTY_FILTER, logCallback11);
    events = logCallback11.getEvents();
    Assert.assertTrue(events.size() > 16);
    Assert.assertEquals("Test log message 18 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 33 arg1 arg2",
                        events.get(events.size() - 1 - (events.size() - 16)).getLoggingEvent().getFormattedMessage());
  }

  private static void publishLogs() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.set(LoggingConfiguration.KAFKA_SEED_BROKERS, "localhost:" + getKafkaPort());
    conf.set(LoggingConfiguration.NUM_PARTITIONS, "2");
    conf.set(LoggingConfiguration.KAFKA_PRODUCER_TYPE, "sync");
    conf.set(LoggingConfiguration.KAFKA_PROCUDER_BUFFER_MS, "100");
    KafkaLogAppender appender = new KafkaLogAppender(conf);
    new LogAppenderInitializer(appender).initialize("LogSaverTest");

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    StatusPrinter.setPrintStream(new PrintStream(bos));
    StatusPrinter.print((LoggerContext) LoggerFactory.getILoggerFactory());

    ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2));
    List<ListenableFuture<?>> futures = Lists.newArrayList();
    futures.add(executor.submit(new LogPublisher(new FlowletLoggingContext("ACCT_1", "APP_1", "FLOW_1", "FLOWLET_1"))));
    futures.add(executor.submit(new LogPublisher(new FlowletLoggingContext("ACCT_2", "APP_2", "FLOW_2", "FLOWLET_2"))));
    futures.add(executor.submit(new LogPublisher(new ServiceLoggingContext("reactor", "services", "metrics"))));

    Futures.allAsList(futures).get();

    System.out.println(bos.toString());

    appender.stop();
  }

  private static class LogPublisher implements Runnable {
    private final LoggingContext loggingContext;

    private LogPublisher(LoggingContext loggingContext) {
      this.loggingContext = loggingContext;
    }

    @Override
    public void run() {
      LoggingContextAccessor.setLoggingContext(loggingContext);

      Logger logger = LoggerFactory.getLogger("LogSaverTest");
      Exception e1 = new Exception("Test Exception1");
      Exception e2 = new Exception("Test Exception2", e1);

      try {
        for (int j = 0; j < 6; ++j) {
          for (int i = 0; i < 10; ++i) {
            logger.warn("Test log message " + (10 * j + i) + " {} {}", "arg1", "arg2", e2);
            TimeUnit.MILLISECONDS.sleep(1);
          }
          TimeUnit.MILLISECONDS.sleep(1200);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }
  }

  private static void waitTillLogSaverDone(String logBaseDir, String filePattern, String logLine) throws Exception {
    long start = System.currentTimeMillis();

    LocationFactory locationFactory = new LocalLocationFactory();

    while (true) {
      String latestFile = getLatestFile(logBaseDir, filePattern);
      if (latestFile != null) {
        AvroFileLogReader logReader = new AvroFileLogReader(new LogSchema().getAvroSchema());
        LogCallback logCallback = new LogCallback();
        logCallback.init();
        logReader.readLog(locationFactory.create(latestFile), Filter.EMPTY_FILTER, 0,
                          Long.MAX_VALUE, Integer.MAX_VALUE, logCallback);
        logCallback.close();
        List<LogEvent> events = logCallback.getEvents();
        if (events.size() > 0) {
          LogEvent event = events.get(events.size() - 1);
          if (event.getLoggingEvent().getFormattedMessage().equals(logLine)) {
            break;
          }
        }
      }

      Assert.assertTrue("Time exceeded", System.currentTimeMillis() - start < 30 * 1000);
      TimeUnit.MILLISECONDS.sleep(30);
    }

    LOG.info("Done waiting!");
    TimeUnit.SECONDS.sleep(1);
  }

  private static String getLatestFile(String logBaseDir, String filePattern) throws Exception {
    String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    File dir = new File(logBaseDir, String.format(filePattern, date));

    if (!dir.exists()) {
      return null;
    }

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
