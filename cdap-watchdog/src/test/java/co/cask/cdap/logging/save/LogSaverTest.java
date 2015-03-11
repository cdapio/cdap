/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.logging.save;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.KafkaConstants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.logging.ApplicationLoggingContext;
import co.cask.cdap.common.logging.ComponentLoggingContext;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.NamespaceLoggingContext;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.logging.SystemLoggingContext;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DefaultDatasetDefinitionRegistry;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import co.cask.cdap.logging.KafkaTestBase;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.appender.kafka.KafkaLogAppender;
import co.cask.cdap.logging.context.FlowletLoggingContext;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.read.AvroFileLogReader;
import co.cask.cdap.logging.read.DistributedLogReader;
import co.cask.cdap.logging.read.LogEvent;
import co.cask.cdap.logging.serialize.LogSchema;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.watchdog.election.MultiLeaderElection;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.runtime.TransactionModules;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.zookeeper.ZKClientService;
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
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static co.cask.cdap.logging.appender.LoggingTester.LogCallback;

/**
 * Test LogSaver and Distributed Log Reader.
 */
@Category(SlowTests.class)
public class LogSaverTest extends KafkaTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(LogSaverTest.class);

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static Injector injector;
  private static TransactionManager txManager;
  private static String logBaseDir;

  @BeforeClass
  public static void startLogSaver() throws Exception {
    final CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, temporaryFolder.newFolder().getAbsolutePath());
    cConf.set(Constants.Zookeeper.QUORUM, getZkConnectString());
    cConf.unset(KafkaConstants.ConfigKeys.ZOOKEEPER_NAMESPACE_CONFIG);
    cConf.set(LoggingConfiguration.KAFKA_SEED_BROKERS, "localhost:" + getKafkaPort());
    cConf.set(LoggingConfiguration.NUM_PARTITIONS, "2");
    cConf.set(LoggingConfiguration.KAFKA_PRODUCER_TYPE, "sync");
    cConf.set(LoggingConfiguration.KAFKA_PROCUDER_BUFFER_MS, "100");
    cConf.set(LoggingConfiguration.LOG_RETENTION_DURATION_DAYS, "10");
    cConf.set(LoggingConfiguration.LOG_MAX_FILE_SIZE_BYTES, "10240");
    cConf.set(LoggingConfiguration.LOG_FILE_SYNC_INTERVAL_BYTES, "5120");
    cConf.set(LoggingConfiguration.LOG_SAVER_EVENT_BUCKET_INTERVAL_MS, "100");
    cConf.set(LoggingConfiguration.LOG_SAVER_MAXIMUM_INMEMORY_EVENT_BUCKETS, "2");
    cConf.set(LoggingConfiguration.LOG_SAVER_TOPIC_WAIT_SLEEP_MS, "10");

    Configuration hConf = HBaseConfiguration.create();

    injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new ZKClientModule(),
      new KafkaClientModule(),
      new LocationRuntimeModule().getInMemoryModules(),
      new TransactionModules().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(new TypeLiteral<Map<String, ? extends DatasetModule>>() { })
            .annotatedWith(Names.named("defaultDatasetModules")).toInstance(Maps.<String, DatasetModule>newHashMap());
          install(new FactoryModuleBuilder()
                    .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                    .build(DatasetDefinitionRegistryFactory.class));
          bind(DatasetFramework.class).to(InMemoryDatasetFramework.class).in(Scopes.SINGLETON);
        }
      });

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    logBaseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR);

    DatasetFramework dsFramework = injector.getInstance(DatasetFramework.class);
    dsFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE_ID, "table"),
                          new InMemoryTableModule());

    ZKClientService zkClientService = injector.getInstance(ZKClientService.class);
    zkClientService.startAndWait();

    KafkaClientService kafkaClient = injector.getInstance(KafkaClientService.class);
    kafkaClient.startAndWait();

    LogSaver logSaver = injector.getInstance(LogSaver.class);
    logSaver.startAndWait();

    MultiLeaderElection multiElection = new MultiLeaderElection(zkClientService, "log-saver", 2, logSaver);
    multiElection.setLeaderElectionSleepMs(1);
    multiElection.startAndWait();

    // Sleep a while to let Kafka server fully initialized.
    TimeUnit.SECONDS.sleep(5);

    publishLogs();

    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    String logBaseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR);
    Location ns1LogBaseDir = locationFactory.create("NS_1").append(logBaseDir);
    Location ns2LogBaseDir = locationFactory.create("NS_2").append(logBaseDir);
    Location systemLogBaseDir = locationFactory.create("system").append(logBaseDir);

    waitTillLogSaverDone(ns1LogBaseDir, "APP_1/flow-FLOW_1/%s", "Test log message 59 arg1 arg2");
    waitTillLogSaverDone(ns2LogBaseDir, "APP_2/flow-FLOW_2/%s", "Test log message 59 arg1 arg2");
    waitTillLogSaverDone(systemLogBaseDir, "services/service-metrics/%s", "Test log message 59 arg1 arg2");

    logSaver.stopAndWait();
    multiElection.stopAndWait();
    kafkaClient.stopAndWait();
    zkClientService.stopAndWait();
  }

  @AfterClass
  public static void testCheckpoint() throws Exception {
    CheckpointManager checkpointManager = injector.getInstance(CheckpointManager.class);
    Assert.assertEquals(120, checkpointManager.getCheckpoint(0));
    Assert.assertEquals(60, checkpointManager.getCheckpoint(1));

    txManager.stopAndWait();
  }

  @Test
  public void testLogRead2() throws Exception {
    testLogRead(new FlowletLoggingContext("NS_1", "APP_1", "FLOW_1", ""));
  }

  @Test
  public void testLogRead1() throws Exception {
    testLogRead(new FlowletLoggingContext("NS_2", "APP_2", "FLOW_2", ""));
  }

  @Test
  public void testLogRead3() throws Exception {
    testLogRead(new ServiceLoggingContext("system", "services", "metrics"));
  }

  private void testLogRead(LoggingContext loggingContext) throws Exception {
    LOG.info("Verifying logging context {}", loggingContext.getLogPathFragment(logBaseDir));
    DistributedLogReader distributedLogReader = injector.getInstance(DistributedLogReader.class);

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
          loggingContext.getSystemTagsMap().get(NamespaceLoggingContext.TAG_NAMESPACE_ID).getValue(),
          allEvents.get(i).getLoggingEvent().getMDCPropertyMap().get(NamespaceLoggingContext.TAG_NAMESPACE_ID));
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
    KafkaLogAppender appender = injector.getInstance(KafkaLogAppender.class);
    new LogAppenderInitializer(appender).initialize("LogSaverTest");

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    StatusPrinter.setPrintStream(new PrintStream(bos));
    StatusPrinter.print((LoggerContext) LoggerFactory.getILoggerFactory());

    ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2));
    List<ListenableFuture<?>> futures = Lists.newArrayList();
    futures.add(executor.submit(new LogPublisher(new FlowletLoggingContext("NS_1", "APP_1", "FLOW_1", "FLOWLET_1"))));
    futures.add(executor.submit(new LogPublisher(new FlowletLoggingContext("NS_2", "APP_2", "FLOW_2", "FLOWLET_2"))));
    futures.add(executor.submit(new LogPublisher(new ServiceLoggingContext("system", "services", "metrics"))));

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
            if (j == 0 && (i <= 3)) {
              // For some events introduce the sleep of more than 8 seconds for windowing to take effect
              TimeUnit.MILLISECONDS.sleep(300);
            } else {
              TimeUnit.MILLISECONDS.sleep(1);
            }
          }
          TimeUnit.MILLISECONDS.sleep(1200);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }
  }

  private static void waitTillLogSaverDone(Location logBaseDir, String filePattern, String logLine) throws Exception {
    long start = System.currentTimeMillis();

    while (true) {
      Location latestFile = getLatestFile(logBaseDir, filePattern);
      if (latestFile != null) {
        AvroFileLogReader logReader = new AvroFileLogReader(new LogSchema().getAvroSchema());
        LogCallback logCallback = new LogCallback();
        logCallback.init();
        logReader.readLog(latestFile, Filter.EMPTY_FILTER, 0, Long.MAX_VALUE, Integer.MAX_VALUE, logCallback);
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

  private static Location getLatestFile(Location logBaseDir, String filePattern) throws Exception {
    String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    Location dir = logBaseDir.append(String.format(filePattern, date));

    if (!dir.exists()) {
      return null;
    }

    List<Location> files = dir.list();
    if (files.isEmpty()) {
      return null;
    }

    SortedMap<Long, Location> map = Maps.newTreeMap();
    for (Location file : files) {
      String filename = FilenameUtils.getBaseName(file.getName());
      map.put(Long.parseLong(filename), file);
    }
    return map.get(map.lastKey());
  }
}
