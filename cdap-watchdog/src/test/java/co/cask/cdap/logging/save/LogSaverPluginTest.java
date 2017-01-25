/*
 * Copyright © 2015-2017 Cask Data, Inc.
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
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.util.StatusPrinter;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.RootLocationFactory;
import co.cask.cdap.common.logging.ApplicationLoggingContext;
import co.cask.cdap.common.logging.ComponentLoggingContext;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.NamespaceLoggingContext;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.logging.KafkaTestBase;
import co.cask.cdap.logging.LogSaverTableUtilOverride;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.appender.kafka.KafkaLogAppender;
import co.cask.cdap.logging.appender.kafka.LoggingEventSerializer;
import co.cask.cdap.logging.context.FlowletLoggingContext;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.read.FileLogReader;
import co.cask.cdap.logging.read.FileMetadataReader;
import co.cask.cdap.logging.read.LogEvent;
import co.cask.cdap.logging.write.LogLocation;
import co.cask.cdap.test.SlowTests;
import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.kafka.client.FetchedMessage;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static co.cask.cdap.logging.appender.LoggingTester.LogCallback;

/**
 * Test LogSaver plugin.
 * TODO (CDAP-3128) This class needs to be refactor with LogSaverTest.
 */
@Category(SlowTests.class)
public class LogSaverPluginTest extends KafkaTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(LogSaverTest.class);

  private static Injector injector;
  private static TransactionManager txManager;
  private static LogSaver logSaver;
  private static KafkaLogAppender appender;
  private static Gson gson;
  private static MetricStore metricStore;
  private static MetricsCollectionService metricsCollectionService;
  private static CConfiguration cConf;

  @BeforeClass
  public static void initialize() throws IOException {
    cConf = KAFKA_TESTER.getCConf();

    injector = KAFKA_TESTER.getInjector();
    appender = injector.getInstance(KafkaLogAppender.class);
    CountingLogAppender countingLogAppender = new CountingLogAppender(appender);
    new LogAppenderInitializer(countingLogAppender).initialize("LogSaverPluginTest");
    gson = new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
    metricStore = injector.getInstance(MetricStore.class);
    metricsCollectionService = injector.getInstance(MetricsCollectionService.class);

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    metricsCollectionService.startAndWait();
  }

  @AfterClass
  public static void shutdown() {
    appender.stop();
    metricsCollectionService.stopAndWait();
    txManager.stopAndWait();
  }

  public void startLogSaver() throws Exception {
    LogSaverFactory factory = injector.getInstance(LogSaverFactory.class);
    logSaver = factory.create(ImmutableSet.of(0));
    logSaver.startAndWait();
  }

  private void stopLogSaver() {
    logSaver.stopAndWait();
  }

  @Test
  public void testPlugin() throws Exception {

    // Start the log saver
    // Publish logs
    // Reset checkpoint for one of the plugins
    // Re-process from the reset offset
    // Verify checkpoints

    try {
      startLogSaver();

      FlowletLoggingContext loggingContext =
        new FlowletLoggingContext("NS_1", "APP_1", "FLOW_1", "", "RUN1", "INSTANCE");
      FileLogReader fileLogReader = injector.getInstance(FileLogReader.class);
      try (CloseableIterator<LogEvent> events =
        fileLogReader.getLog(loggingContext, 0, Long.MAX_VALUE, Filter.EMPTY_FILTER)) {
        Assert.assertFalse(events.hasNext());
      }

      publishLogs();

      String logBaseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR);

      FileMetadataReader fileMetadataReader =
        new FileMetadataReader(injector.getInstance(DatasetFramework.class),
                               injector.getInstance(TransactionSystemClient.class),
                               injector.getInstance(RootLocationFactory.class),
                               injector.getInstance(Impersonator.class),
                               "log.meta");

      waitTillLogSaverDone(fileMetadataReader, loggingContext, "Test log message 59 arg1 arg2");

      testLogRead(loggingContext, logBaseDir);

      List<LogEvent> events = Lists.newArrayList(
        fileLogReader.getLog(new FlowletLoggingContext("NS_1", "APP_1", "FLOW_1", "", null, "INSTANCE"),
                             0, Long.MAX_VALUE, Filter.EMPTY_FILTER));
      Assert.assertEquals(60, events.size());

      stopLogSaver();

      // Checkpoint should read 60 for both processor
      verifyCheckpoint();
      verifyMetricsPlugin(60L);

      // Now reset the logsaver to start reading from reset offset
      // Change the log meta table so that checkpoints are new, and old log files are not read during read later
      LogSaverTableUtilOverride.setLogMetaTableName("log.meta1");
      fileMetadataReader =
        new FileMetadataReader(injector.getInstance(DatasetFramework.class),
                               injector.getInstance(TransactionSystemClient.class),
                               injector.getInstance(RootLocationFactory.class),
                               injector.getInstance(Impersonator.class),
                               "log.meta1");


      // Reset checkpoint for log saver plugin
      resetLogSaverPluginCheckpoint(10);

      // Change base dir so that new files get written and read from a different location (makes it easy for asserting)
      String logBaseDir1 = logBaseDir + "1";
      cConf.set(LoggingConfiguration.LOG_BASE_DIR, logBaseDir1);

      // Start log saver again
      startLogSaver();
      // Reset the log base dir back to original value
      cConf.set(LoggingConfiguration.LOG_BASE_DIR, logBaseDir);

      waitTillLogSaverDone(fileMetadataReader, loggingContext, "Test log message 59 arg1 arg2");
      stopLogSaver();


      //Verify that more records are processed by LogWriter plugin
      fileLogReader = new FileLogReader(fileMetadataReader);
      events =
        Lists.newArrayList(fileLogReader.getLog(new FlowletLoggingContext("NS_1", "APP_1", "FLOW_1", "",
                                                                          null, "INSTANCE"),
                                                0, Long.MAX_VALUE, Filter.EMPTY_FILTER));
      // Since we started from offset 10, we should have only saved 50 messages this time
      Assert.assertEquals(50, events.size());
      // Checkpoint should read 60 for both processor
      verifyCheckpoint();
      // Metrics should be 60 for first run + 50 for second run
      verifyMetricsPlugin(110L);
    } catch (Throwable t) {
      try {
        final Multimap<String, String> contextMessages = getPublishedKafkaMessages();
        LOG.info("All kafka messages: {}", contextMessages);
      } catch (Throwable e) {
        LOG.error("Error while getting published kafka messages {}", e);
      }
      throw t;
    }
  }

  private void verifyMetricsPlugin(long count) throws Exception {
    final long timeInSecs = System.currentTimeMillis() / 1000;
    final Map<String, String> sliceByTags = new HashMap<>();
    sliceByTags.put(Constants.Metrics.Tag.NAMESPACE, "NS_1");
    sliceByTags.put(Constants.Metrics.Tag.APP, "APP_1");
    sliceByTags.put(Constants.Metrics.Tag.FLOW, "FLOW_1");

    // Metrics aggregation may take some time
    Tasks.waitFor(count, new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        // Since we are querying from Integer.MAX_VALUE metrics table,
        // there is only one data point so time range in query does not matter
        Collection<MetricTimeSeries> metricTimeSeries = metricStore.query(
          new MetricDataQuery(0, timeInSecs, Integer.MAX_VALUE, "system.app.log.warn", AggregationFunction.SUM,
                              sliceByTags, ImmutableList.<String>of()));
        return metricTimeSeries.isEmpty() ? 0L : metricTimeSeries.iterator().next().getTimeValues().get(0).getValue();

      }
    }, 120, TimeUnit.SECONDS);
  }

  private void resetLogSaverPluginCheckpoint(long offset) throws Exception {
    TypeLiteral<Set<KafkaLogProcessorFactory>> type = new TypeLiteral<Set<KafkaLogProcessorFactory>>() { };
    Set<KafkaLogProcessorFactory> processorFactories =
      injector.getInstance(Key.get(type, Names.named(Constants.LogSaver.MESSAGE_PROCESSOR_FACTORIES)));

    for (KafkaLogProcessorFactory processorFactory : processorFactories) {
      KafkaLogProcessor processor = processorFactory.create();
      if (processor instanceof  KafkaLogWriterPlugin) {
        KafkaLogWriterPlugin plugin = (KafkaLogWriterPlugin) processor;
        CheckpointManager manager = plugin.getCheckPointManager();
        manager.saveCheckpoints(ImmutableMap.of(0, new Checkpoint(offset, -1)));
      }
    }
  }

  public void verifyCheckpoint() throws Exception {
    TypeLiteral<Set<KafkaLogProcessorFactory>> type = new TypeLiteral<Set<KafkaLogProcessorFactory>>() { };
    Set<KafkaLogProcessorFactory> processorFactories =
      injector.getInstance(Key.get(type, Names.named(Constants.LogSaver.MESSAGE_PROCESSOR_FACTORIES)));

    for (KafkaLogProcessorFactory processorFactory : processorFactories) {
      KafkaLogProcessor processor = processorFactory.create();
      CheckpointManager checkpointManager = getCheckPointManager(processor);
      Assert.assertEquals("Checkpoint mismatch for " + processor.getClass().getName(),
                          60, checkpointManager.getCheckpoint(0).getNextOffset());
    }
  }

  private static CheckpointManager getCheckPointManager(KafkaLogProcessor processor) {
    if (processor instanceof KafkaLogWriterPlugin) {
      KafkaLogWriterPlugin plugin = (KafkaLogWriterPlugin) processor;
      return plugin.getCheckPointManager();
    } else if (processor instanceof LogMetricsPlugin) {
      LogMetricsPlugin plugin = (LogMetricsPlugin) processor;
      return plugin.getCheckPointManager();
    }
    throw new IllegalArgumentException("Invalid processor");
  }

  private void testLogRead(LoggingContext loggingContext, String logBaseDir) throws Exception {
    LOG.info("Verifying logging context {}", loggingContext.getLogPathFragment(logBaseDir));
    FileLogReader distributedLogReader = injector.getInstance(FileLogReader.class);

    List<LogEvent> allEvents =
      Lists.newArrayList(distributedLogReader.getLog(loggingContext, 0, Long.MAX_VALUE, Filter.EMPTY_FILTER));

    for (int i = 0; i < 60; ++i) {
      Assert.assertEquals("Assert failed: ", String.format("Test log message %d arg1 arg2", i),
                          allEvents.get(i).getLoggingEvent().getFormattedMessage());
      if (loggingContext instanceof ServiceLoggingContext) {
        Assert.assertEquals(
          loggingContext.getSystemTagsMap().get(NamespaceLoggingContext.TAG_NAMESPACE_ID).getValue(),
          allEvents.get(i).getLoggingEvent().getMDCPropertyMap().get(NamespaceLoggingContext.TAG_NAMESPACE_ID));
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

    List<LogEvent> events = Lists.newArrayList(
      distributedLogReader.getLog(loggingContext, allEvents.get(10).getLoggingEvent().getTimeStamp(),
                                  allEvents.get(20).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER));
    Assert.assertEquals(10, events.size());
    Assert.assertEquals("Test log message 10 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 19 arg1 arg2", events.get(9).getLoggingEvent().getFormattedMessage());

    events = Lists.newArrayList(
      distributedLogReader.getLog(loggingContext, allEvents.get(5).getLoggingEvent().getTimeStamp(),
                                  allEvents.get(55).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER));
    Assert.assertEquals(50, events.size());
    Assert.assertEquals("Test log message 5 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 54 arg1 arg2", events.get(49).getLoggingEvent().getFormattedMessage());

    events = Lists.newArrayList(
      distributedLogReader.getLog(loggingContext, allEvents.get(30).getLoggingEvent().getTimeStamp(),
                                  allEvents.get(53).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER));
    Assert.assertEquals(23, events.size());
    Assert.assertEquals("Test log message 30 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 52 arg1 arg2", events.get(22).getLoggingEvent().getFormattedMessage());

    events = Lists.newArrayList(
      distributedLogReader.getLog(loggingContext, allEvents.get(35).getLoggingEvent().getTimeStamp(),
                                  allEvents.get(38).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER));
    Assert.assertEquals(3, events.size());
    Assert.assertEquals("Test log message 35 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 37 arg1 arg2", events.get(2).getLoggingEvent().getFormattedMessage());

    events = Lists.newArrayList(
      distributedLogReader.getLog(loggingContext, allEvents.get(53).getLoggingEvent().getTimeStamp(),
                                  allEvents.get(59).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER));
    Assert.assertEquals(6, events.size());
    Assert.assertEquals("Test log message 53 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 58 arg1 arg2", events.get(5).getLoggingEvent().getFormattedMessage());

    events = Lists.newArrayList(
      distributedLogReader.getLog(loggingContext, allEvents.get(59).getLoggingEvent().getTimeStamp(),
                                  allEvents.get(59).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER));
    Assert.assertEquals(0, events.size());

    events = Lists.newArrayList(
      distributedLogReader.getLog(loggingContext, allEvents.get(0).getLoggingEvent().getTimeStamp(),
                                  allEvents.get(0).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER));
    Assert.assertEquals(0, events.size());

    events = Lists.newArrayList(
      distributedLogReader.getLog(loggingContext, allEvents.get(20).getLoggingEvent().getTimeStamp(),
                                  allEvents.get(20).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER));
    Assert.assertEquals(0, events.size());

    events = Lists.newArrayList(
      distributedLogReader.getLog(loggingContext, allEvents.get(32).getLoggingEvent().getTimeStamp() - 999999,
                                  allEvents.get(45).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER));
    Assert.assertTrue(events.size() > 13);
    Assert.assertEquals("Test log message 32 arg1 arg2",
                        events.get(events.size() - 13).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 44 arg1 arg2",
                        events.get(events.size() - 1).getLoggingEvent().getFormattedMessage());

    events = Lists.newArrayList(
      distributedLogReader.getLog(loggingContext, allEvents.get(18).getLoggingEvent().getTimeStamp(),
                                  allEvents.get(34).getLoggingEvent().getTimeStamp() + 999999,
                                  Filter.EMPTY_FILTER));
    Assert.assertTrue(events.size() > 16);
    Assert.assertEquals("Test log message 18 arg1 arg2", events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals("Test log message 33 arg1 arg2",
                        events.get(events.size() - 1 - (events.size() - 16)).getLoggingEvent().getFormattedMessage());
  }

  private Multimap<String, String> getPublishedKafkaMessages() throws InterruptedException {
    final Multimap<String, String> contextMessages = ArrayListMultimap.create();
    KAFKA_TESTER.getPublishedMessages(KAFKA_TESTER.getCConf().get(Constants.Logging.KAFKA_TOPIC),
                                      ImmutableSet.of(0, 1), 60, 0, new Function<FetchedMessage, String>() {
        @Override
        public String apply(final FetchedMessage input) {
          try {
            Map.Entry<String, String> entry = convertFetchedMessage(input);
            contextMessages.put(entry.getKey(), entry.getValue());
          } catch (IOException e) {
            LOG.error("Error while converting FetchedMessage {}", e);
          }
          return "";
        }
      });
    for (Map.Entry<String, Collection<String>> entry : contextMessages.asMap().entrySet()) {
      LOG.info("Kafka Message Count for {} is {}", entry.getKey(), entry.getValue().size());
    }
    return contextMessages;
  }

  private Map.Entry<String, String> convertFetchedMessage(FetchedMessage message) throws IOException {
    LoggingEventSerializer serializer = new LoggingEventSerializer();
    ILoggingEvent iLoggingEvent = serializer.fromBytes(message.getPayload());
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(iLoggingEvent.getMDCPropertyMap());
    String key = loggingContext.getLogPartition();

    // Temporary map for pretty format
    Map<String, String> tempMap = new HashMap<>();
    tempMap.put("Timestamp", Long.toString(iLoggingEvent.getTimeStamp()));
    tempMap.put("Partition", Long.toString(message.getTopicPartition().getPartition()));
    tempMap.put("LogEvent", iLoggingEvent.getFormattedMessage());
    return Maps.immutableEntry(key, gson.toJson(tempMap));
  }

  private void publishLogs() throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    StatusPrinter.setPrintStream(new PrintStream(bos));
    StatusPrinter.print((LoggerContext) LoggerFactory.getILoggerFactory());

    ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    List<ListenableFuture<?>> futures = Lists.newArrayList();
    futures.add(executor.submit(new LogPublisher(0, new FlowletLoggingContext("NS_1", "APP_1", "FLOW_1", "FLOWLET_1",
                                                                              "RUN1", "INSTANCE1"))));
    Futures.allAsList(futures).get();
    executor.shutdownNow();
  }

  private static class LogPublisher implements Runnable {
    private final int startIndex;
    private final LoggingContext loggingContext;

    private LogPublisher(int startIndex, LoggingContext loggingContext) {
      this.startIndex = startIndex;
      this.loggingContext = loggingContext;
    }

    @Override
    public void run() {
      LoggingContextAccessor.setLoggingContext(loggingContext);

      Logger logger = LoggerFactory.getLogger("LogSaverPluginTest");
      Exception e1 = new Exception("Test Exception1");
      Exception e2 = new Exception("Test Exception2", e1);

      int startBatch = startIndex / 10;
      try {
        for (int j = startBatch; j < startBatch + 6; ++j) {
          for (int i = 0; i < 10; ++i) {
            logger.warn("Test log message " + (10 * j + i) + " {} {}", "arg1", "arg2", e2);
            if (j == startIndex && (i <= 3)) {
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

  private static void waitTillLogSaverDone(FileMetadataReader fileMetadataReader, LoggingContext loggingContext,
                                           String logLine) throws Exception {
    long start = System.currentTimeMillis();

    while (true) {
      List<LogLocation> files =
        fileMetadataReader.listFiles(LoggingContextHelper.getLogPathIdentifier(loggingContext), 0, Long.MAX_VALUE);
      if (files.isEmpty()) {
        continue;
      }
      LogLocation lastEntry = files.get(files.size() - 1);
      if (lastEntry != null) {
        LogCallback logCallback = new LogCallback();
        logCallback.init();
        lastEntry.readLog(Filter.EMPTY_FILTER, 0, Long.MAX_VALUE, Integer.MAX_VALUE, logCallback);
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
}
