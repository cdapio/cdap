/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.logging.appender;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryTableService;
import co.cask.cdap.logging.KafkaTestBase;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.appender.file.FileLogAppender;
import co.cask.cdap.logging.appender.kafka.KafkaLogAppender;
import co.cask.cdap.logging.appender.kafka.KafkaTopic;
import co.cask.cdap.logging.context.FlowletLoggingContext;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.logging.read.DistributedLogReader;
import co.cask.cdap.logging.read.FileLogReader;
import co.cask.cdap.logging.read.LogEvent;
import co.cask.cdap.logging.read.LogOffset;
import co.cask.cdap.logging.read.ReadRange;
import co.cask.cdap.logging.save.Checkpoint;
import co.cask.cdap.logging.save.CheckpointManager;
import co.cask.cdap.logging.save.CheckpointManagerFactory;
import co.cask.cdap.logging.save.KafkaLogWriterPlugin;
import co.cask.cdap.test.SlowTests;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.runtime.TransactionModules;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests Distributed Log Reader.
 */
@Category(SlowTests.class)
public class TestDistributedLogReader extends KafkaTestBase {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static final FlowletLoggingContext LOGGING_CONTEXT =
    new FlowletLoggingContext("TDL_NS_1", "APP_1", "FLOW_1", "FLOWLET_1", "RUN1", "INSTANCE1");

  private static Injector injector;
  private static TransactionManager txManager;

  @BeforeClass
  public static void setUpContext() throws Exception {
    Configuration hConf = HBaseConfiguration.create();
    final CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setInt(LoggingConfiguration.LOG_MAX_FILE_SIZE_BYTES, 20 * 1024);
    cConf.set(LoggingConfiguration.KAFKA_SEED_BROKERS, "localhost:" + KafkaTestBase.getKafkaPort());
    cConf.set(LoggingConfiguration.NUM_PARTITIONS, "1");
    cConf.set(LoggingConfiguration.KAFKA_PRODUCER_TYPE, "sync");
    String logBaseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR) + "/" +
      TestDistributedLogReader.class.getSimpleName();
    cConf.set(LoggingConfiguration.LOG_BASE_DIR, logBaseDir);

    injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new LocationRuntimeModule().getInMemoryModules(),
      new TransactionModules().getInMemoryModules(),
      new LoggingModules().getInMemoryModules(),
      new DataSetsModules().getInMemoryModules(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
        }
      }
    );

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();

    LoggingContextAccessor.setLoggingContext(LOGGING_CONTEXT);

    LogAppender fileAppender = injector.getInstance(FileLogAppender.class);

    new LogAppenderInitializer(fileAppender).initialize("TestDistributedLogReader-file");
    Logger fileLogger = LoggerFactory.getLogger("TestDistributedLogReader-file");
    generateLogs(fileLogger, 0, 20);
    fileAppender.stop();

    fileAppender = injector.getInstance(FileLogAppender.class);
    LogAppender kafkaAppender = injector.getInstance(KafkaLogAppender.class);
    new LogAppenderInitializer(fileAppender).initialize("TestDistributedLogReader-both");
    new LogAppenderInitializer(kafkaAppender).initialize("TestDistributedLogReader-both");
    Logger bothLogger = LoggerFactory.getLogger("TestDistributedLogReader-both");
    generateLogs(bothLogger, 20, 10);
    fileAppender.stop();
    kafkaAppender.stop();

    generateCheckpointTime(30);

    kafkaAppender = injector.getInstance(KafkaLogAppender.class);
    new LogAppenderInitializer(kafkaAppender).initialize("TestDistributedLogReader-kafka");
    Logger kafkaLogger = LoggerFactory.getLogger("TestDistributedLogReader-kafka");
    generateLogs(kafkaLogger, 30, 30);

    kafkaAppender.stop();
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    InMemoryTableService.reset();
    txManager.stopAndWait();
  }

  @Test
  public void testDistributedLogPrev() throws Exception {
    DistributedLogReader distributedLogReader = injector.getInstance(DistributedLogReader.class);
    int count = 60;
    ReadRange readRange = ReadRange.LATEST;
    for (int i = 0; i < 15; ++i) {
      LoggingTester.LogCallback callback = new LoggingTester.LogCallback();
      distributedLogReader.getLogPrev(LOGGING_CONTEXT, readRange, 4, Filter.EMPTY_FILTER, callback);
      List<LogEvent> events = callback.getEvents();
      Assert.assertFalse(events.isEmpty());
      readRange = ReadRange.createToRange(events.get(0).getOffset());

      Collections.reverse(events);
      for (LogEvent event : events) {
        Assert.assertEquals("TestDistributedLogReader Log message " + --count,
                            event.getLoggingEvent().getFormattedMessage());
      }
    }
    Assert.assertEquals(0, count);
  }

  @Test
  public void testDistributedLogNext() throws Exception {
    DistributedLogReader distributedLogReader = injector.getInstance(DistributedLogReader.class);
    int count = 0;
    ReadRange readRange = new ReadRange(0, Long.MAX_VALUE, LogOffset.INVALID_KAFKA_OFFSET);
    for (int i = 0; i < 20; ++i) {
      LoggingTester.LogCallback callback = new LoggingTester.LogCallback();
      distributedLogReader.getLogNext(LOGGING_CONTEXT, readRange, 3, Filter.EMPTY_FILTER, callback);
      List<LogEvent> events = callback.getEvents();
      Assert.assertFalse(events.isEmpty());
      readRange = ReadRange.createFromRange(events.get(events.size() - 1).getOffset());

      for (LogEvent event : events) {
        Assert.assertEquals("TestDistributedLogReader Log message " + count++,
                            event.getLoggingEvent().getFormattedMessage());
      }
    }
    Assert.assertEquals(60, count);
  }

  private static void generateLogs(Logger logger, int start, int max) throws InterruptedException {
    for (int i = start; i < start + max; ++i) {
      logger.warn("TestDistributedLogReader Log message {}", i);
      TimeUnit.MILLISECONDS.sleep(1);
    }
  }

  private static void generateCheckpointTime(int numExpectedEvents) throws Exception {
    FileLogReader logReader = injector.getInstance(FileLogReader.class);
    LoggingTester.LogCallback logCallback = new LoggingTester.LogCallback();
    logReader.getLog(LOGGING_CONTEXT, 0, Long.MAX_VALUE, Filter.EMPTY_FILTER, logCallback);
    Assert.assertEquals(numExpectedEvents, logCallback.getEvents().size());

    // Save checkpoint (time of last event)
    CheckpointManagerFactory checkpointManagerFactory = injector.getInstance(CheckpointManagerFactory.class);
    CheckpointManager checkpointManager =
      checkpointManagerFactory.create(KafkaTopic.getTopic(), KafkaLogWriterPlugin.CHECKPOINT_ROW_KEY_PREFIX);
    long checkpointTime = logCallback.getEvents().get(numExpectedEvents - 1).getLoggingEvent().getTimeStamp();
    checkpointManager.saveCheckpoint(0, new Checkpoint(numExpectedEvents, checkpointTime));
  }
}
