/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.ApplicationLoggingContext;
import co.cask.cdap.common.logging.ComponentLoggingContext;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.logging.NamespaceLoggingContext;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryTableService;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.logging.KafkaTestBase;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.appender.kafka.KafkaLogAppender;
import co.cask.cdap.logging.appender.kafka.LoggingEventSerializer;
import co.cask.cdap.logging.context.FlowletLoggingContext;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.read.AvroFileReader;
import co.cask.cdap.logging.read.FileLogReader;
import co.cask.cdap.logging.read.LogEvent;
import co.cask.cdap.logging.serialize.LogSchema;
import co.cask.cdap.logging.write.FileMetaDataManager;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.SlowTests;
import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
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
import org.apache.twill.filesystem.Location;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static co.cask.cdap.logging.appender.LoggingTester.LogCallback;

/**
 * Test LogSaver and Distributed Log Reader.
 */
@Category(SlowTests.class)
public class LogSaverTest extends KafkaTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(LogSaverTest.class);

  private static Injector injector;
  private static TransactionManager txManager;
  private static String logBaseDir;
  private static KafkaLogAppender appender;
  private static Gson gson;
  private static Impersonator impersonator;

  @BeforeClass
  public static void startLogSaver() throws Exception {
    CConfiguration cConf = KAFKA_TESTER.getCConf();
    logBaseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR) + "/" + LogSaverTest.class.getSimpleName();
    cConf.set(LoggingConfiguration.LOG_BASE_DIR, logBaseDir);

    injector = KAFKA_TESTER.getInjector();
    impersonator = injector.getInstance(Impersonator.class);
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();

    appender = injector.getInstance(KafkaLogAppender.class);
    CountingLogAppender countingLogAppender = new CountingLogAppender(appender);
    new LogAppenderInitializer(countingLogAppender).initialize("LogSaverTest");
    gson = new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();

    LogSaverFactory factory = injector.getInstance(LogSaverFactory.class);
    // {0, 1} - because we have 2 partitions as per configuration above (see LoggingConfiguration.NUM_PARTITIONS)
    LogSaver logSaver = factory.create(ImmutableSet.of(0, 1));
    logSaver.startAndWait();

    publishLogs();

    FileMetaDataManager fileMetaDataManager = injector.getInstance(FileMetaDataManager.class);
    waitTillLogSaverDone(fileMetaDataManager,
                         new FlowletLoggingContext("NS_1", "APP_1", "FLOW_1", "", null, "INSTANCE"),
                         "Test log message 119 arg1 arg2");
    waitTillLogSaverDone(fileMetaDataManager,
                         new FlowletLoggingContext("NS_2", "APP_2", "FLOW_2", "", null, "INSTANCE"),
                         "Test log message 119 arg1 arg2");
    waitTillLogSaverDone(fileMetaDataManager, new ServiceLoggingContext("system", "services", "metrics"),
                         "Test log message 59 arg1 arg2");

    logSaver.stopAndWait();
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    appender.stop();
    InMemoryTableService.reset();
    txManager.stopAndWait();
  }

  @Test
  public void testCheckpoint() throws Exception {
    TypeLiteral<Set<KafkaLogProcessorFactory>> type = new TypeLiteral<Set<KafkaLogProcessorFactory>>() {
    };
    Set<KafkaLogProcessorFactory> processorFactories =
      injector.getInstance(Key.get(type, Names.named(Constants.LogSaver.MESSAGE_PROCESSOR_FACTORIES)));

    try {
      for (KafkaLogProcessorFactory processorFactory : processorFactories) {
        KafkaLogProcessor processor = processorFactory.create();
        CheckpointManager checkpointManager = getCheckPointManager(processor);

        // Verify checkpoint offset
        Assert.assertEquals(180, checkpointManager.getCheckpoint(0).getNextOffset());
        Assert.assertEquals(120, checkpointManager.getCheckpoint(1).getNextOffset());

        // Verify checkpoint time
        // Read with null runid should give 120 results back
        long checkpointTimeApp1 =
          getCheckpointTime(new FlowletLoggingContext("NS_1", "APP_1", "FLOW_1", "", null, "INSTANCE"), 120);
        long checkpointTimeApp2 =
          getCheckpointTime(new FlowletLoggingContext("NS_2", "APP_2", "FLOW_2", "", null, "INSTANCE"), 120);
        long checkpointTimeService = getCheckpointTime(new ServiceLoggingContext("system", "services", "metrics"), 60);

        // Checkpoint time should be within last 10 minutes
        long currentTime = System.currentTimeMillis();
        Assert.assertTrue(checkpointTimeApp1 > currentTime - TimeUnit.MINUTES.toMillis(10));
        Assert.assertTrue(checkpointTimeApp2 > currentTime - TimeUnit.MINUTES.toMillis(10));
        Assert.assertTrue(checkpointTimeService > currentTime - TimeUnit.MINUTES.toMillis(10));

        // Saved checkpoint must be equal to time on last message for a partition.
        Assert.assertEquals(checkpointTimeApp1, checkpointManager.getCheckpoint(0).getMaxEventTime());
        Assert.assertTrue(checkpointManager.getCheckpoint(1).getMaxEventTime() == checkpointTimeApp2 ||
                            checkpointManager.getCheckpoint(1).getMaxEventTime() == checkpointTimeService);
      }
    } catch (Throwable t) {
      try {
        final Multimap<String, String> contextMessages = getPublishedKafkaMessages();
        LOG.error("All kafka messages: {}", contextMessages);
      } catch (Exception e) {
        LOG.error("Error while getting published kafka messages {}", e);
      }
      throw t;
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


  @Test
  public void testLogRead1() throws Exception {
    testLogRead(new FlowletLoggingContext("NS_1", "APP_1", "FLOW_1", "", "RUN1", "INSTANCE"));
  }

  @Test
  public void testLogRead2() throws Exception {
    testLogRead(new FlowletLoggingContext("NS_2", "APP_2", "FLOW_2", "", "RUN1", "INSTANCE"));
  }

  @Test
  public void testLogRead3() throws Exception {
    testLogRead(new ServiceLoggingContext("system", "services", "metrics"));
  }

  private long getCheckpointTime(LoggingContext loggingContext, int numExpectedEvents) throws Exception {
    FileLogReader distributedLogReader = injector.getInstance(FileLogReader.class);
    ArrayList<LogEvent> events =
      Lists.newArrayList(distributedLogReader.getLog(loggingContext, 0, Long.MAX_VALUE, Filter.EMPTY_FILTER));
    Assert.assertEquals(numExpectedEvents, events.size());

    return events.get(numExpectedEvents - 1).getLoggingEvent().getTimeStamp();
  }

  private void testLogRead(LoggingContext loggingContext) throws Exception {
    LOG.info("Verifying logging context {}", loggingContext.getLogPathFragment(logBaseDir));
    FileLogReader distributedLogReader = injector.getInstance(FileLogReader.class);

    List<LogEvent> allEvents =
      Lists.newArrayList(distributedLogReader.getLog(loggingContext, 0, Long.MAX_VALUE, Filter.EMPTY_FILTER));

    final Multimap<String, String> contextMessages = getPublishedKafkaMessages();

    for (int i = 0; i < 60; ++i) {
      Assert.assertEquals("All messages in Kafka = " + gson.toJson(contextMessages.asMap()),
                          String.format("Test log message %d arg1 arg2", i),
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
                                      ImmutableSet.of(0, 1), 300, 0, new Function<FetchedMessage, String>() {
        @Override
        public String apply(final FetchedMessage input) {
          try {
            Map.Entry<String, String> entry = convertFetchedMessage(input);
            contextMessages.put(entry.getKey(), entry.getValue());
          } catch (IOException e) {
            LOG.error("Error while converting FetchedMessage {} ", e);
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

  private static void publishLogs() throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    StatusPrinter.setPrintStream(new PrintStream(bos));
    StatusPrinter.print((LoggerContext) LoggerFactory.getILoggerFactory());

    ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(3));
    List<ListenableFuture<?>> futures = Lists.newArrayList();
    futures.add(executor.submit(new LogPublisher(0, new FlowletLoggingContext("NS_1", "APP_1", "FLOW_1", "FLOWLET_1",
                                                                              "RUN1", "INSTANCE1"))));
    futures.add(executor.submit(new LogPublisher(0, new FlowletLoggingContext("NS_2", "APP_2", "FLOW_2", "FLOWLET_2",
                                                                              "RUN1", "INSTANCE1"))));
    futures.add(executor.submit(new LogPublisher(0, new ServiceLoggingContext("system", "services", "metrics"))));

    // Make sure the final segments of logs are added at end to simplify checking for done in waitTillLogSaverDone
    futures.add(executor.submit(new LogPublisher(60, new FlowletLoggingContext("NS_1", "APP_1", "FLOW_1", "FLOWLET_1",
                                                                               "RUN2", "INSTANCE2"))));
    futures.add(executor.submit(new LogPublisher(60, new FlowletLoggingContext("NS_2", "APP_2", "FLOW_2", "FLOWLET_2",
                                                                               "RUN2", "INSTANCE2"))));

    Futures.allAsList(futures).get();

    System.out.println(bos.toString());

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

      Logger logger = LoggerFactory.getLogger("LogSaverTest");
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

  private static void waitTillLogSaverDone(FileMetaDataManager fileMetaDataManager, LoggingContext loggingContext,
                                           String logLine) throws Exception {
    long start = System.currentTimeMillis();

    while (true) {
      NavigableMap<Long, Location> files = fileMetaDataManager.listFiles(loggingContext);
      Map.Entry<Long, Location> lastEntry = files.lastEntry();
      if (lastEntry != null) {
        Location latestFile = lastEntry.getValue();
        AvroFileReader logReader = new AvroFileReader(new LogSchema().getAvroSchema());
        LogCallback logCallback = new LogCallback();
        logCallback.init();
        NamespaceId namespaceId = LoggingContextHelper.getNamespaceId(loggingContext);
        logReader.readLog(latestFile, Filter.EMPTY_FILTER, 0, Long.MAX_VALUE, Integer.MAX_VALUE, logCallback,
                          namespaceId, impersonator);
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
