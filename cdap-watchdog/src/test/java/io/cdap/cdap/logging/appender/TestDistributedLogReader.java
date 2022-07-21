/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.logging.appender;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.data2.dataset2.lib.table.inmemory.InMemoryTableService;
import io.cdap.cdap.logging.KafkaTestBase;
import io.cdap.cdap.logging.LoggingConfiguration;
import io.cdap.cdap.logging.appender.kafka.KafkaLogAppender;
import io.cdap.cdap.logging.appender.kafka.StringPartitioner;
import io.cdap.cdap.logging.context.WorkerLoggingContext;
import io.cdap.cdap.logging.filter.Filter;
import io.cdap.cdap.logging.framework.local.LocalLogAppender;
import io.cdap.cdap.logging.meta.Checkpoint;
import io.cdap.cdap.logging.meta.CheckpointManager;
import io.cdap.cdap.logging.meta.KafkaCheckpointManager;
import io.cdap.cdap.logging.meta.KafkaOffset;
import io.cdap.cdap.logging.read.DistributedLogReader;
import io.cdap.cdap.logging.read.FileLogReader;
import io.cdap.cdap.logging.read.LogEvent;
import io.cdap.cdap.logging.read.LogOffset;
import io.cdap.cdap.logging.read.ReadRange;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.store.StoreDefinition;
import io.cdap.cdap.test.SlowTests;
import org.apache.tephra.TransactionManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests Distributed Log Reader.
 */
@Category(SlowTests.class)
public class TestDistributedLogReader extends KafkaTestBase {
  private static final WorkerLoggingContext LOGGING_CONTEXT_BOTH =
    new WorkerLoggingContext("TDL_NS_1", "APP_1", "FLOW_1", "RUN1", "INSTANCE1");

  // Note: LOGGING_CONTEXT_FILE should be the only logging context in partition 0
  private static final WorkerLoggingContext LOGGING_CONTEXT_FILE =
    new WorkerLoggingContext("TDL_NS_2", "APP_2", "FLOW_2", "RUN2", "INSTANCE2");

  private static final WorkerLoggingContext LOGGING_CONTEXT_KAFKA =
    new WorkerLoggingContext("TDL_NS_3", "APP_3", "FLOW_3", "RUN3", "INSTANCE3");

  private static StringPartitioner stringPartitioner;
  private static Injector injector;
  private static TransactionManager txManager;

  @BeforeClass
  public static void setUpContext() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(LoggingConfiguration.LOG_MAX_FILE_SIZE_BYTES, 20 * 1024);
    cConf.set(Constants.Logging.NUM_PARTITIONS, "2");
    cConf.set(LoggingConfiguration.KAFKA_PRODUCER_TYPE, "sync");
    String logBaseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR) + "/" +
      TestDistributedLogReader.class.getSimpleName();
    cConf.set(LoggingConfiguration.LOG_BASE_DIR, logBaseDir);

    injector = KAFKA_TESTER.getInjector();

    stringPartitioner = new StringPartitioner(cConf);
    // NOTE: this test relies on LOGGING_CONTEXT_FILE going into its own kafka partition.
    // Unless the partitioner has changed, or more logging contexts added there should be no issue.
    Assert.assertEquals(1, stringPartitioner.partition(LOGGING_CONTEXT_BOTH.getLogPartition(), -1));
    Assert.assertEquals(0, stringPartitioner.partition(LOGGING_CONTEXT_FILE.getLogPartition(), -1));
    Assert.assertEquals(1, stringPartitioner.partition(LOGGING_CONTEXT_KAFKA.getLogPartition(), -1));

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();

    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class));

    // Generate logs for LOGGING_CONTEXT_BOTH, that contains logs in file, both file and kafka, and only in kafka
    LoggingContextAccessor.setLoggingContext(LOGGING_CONTEXT_BOTH);

    LogAppender fileAppender = injector.getInstance(LocalLogAppender.class);

    new LogAppenderInitializer(fileAppender).initialize("TestDistributedLogReader-file");
    Logger fileLogger = LoggerFactory.getLogger("TestDistributedLogReader-file");
    generateLogs(fileLogger, "Log message1", 0, 20);
    fileAppender.stop();

    fileAppender = injector.getInstance(LocalLogAppender.class);
    LogAppender kafkaAppender = injector.getInstance(KafkaLogAppender.class);
    new LogAppenderInitializer(fileAppender).initialize("TestDistributedLogReader-both");
    new LogAppenderInitializer(kafkaAppender).initialize("TestDistributedLogReader-both");
    Logger bothLogger = LoggerFactory.getLogger("TestDistributedLogReader-both");
    generateLogs(bothLogger, "Log message1", 20, 10);
    fileAppender.stop();
    kafkaAppender.stop();

    generateCheckpointTime(LOGGING_CONTEXT_BOTH, 30, cConf.get(Constants.Logging.KAFKA_TOPIC));

    kafkaAppender = injector.getInstance(KafkaLogAppender.class);
    new LogAppenderInitializer(kafkaAppender).initialize("TestDistributedLogReader-kafka");
    Logger kafkaLogger = LoggerFactory.getLogger("TestDistributedLogReader-kafka");
    generateLogs(kafkaLogger, "Log message1", 30, 30);

    kafkaAppender.stop();

    // Generate logs for LOGGING_CONTEXT_FILE, logs only in file
    LoggingContextAccessor.setLoggingContext(LOGGING_CONTEXT_FILE);
    fileAppender = injector.getInstance(LocalLogAppender.class);
    new LogAppenderInitializer(fileAppender).initialize("TestDistributedLogReader-file-2");
    fileLogger = LoggerFactory.getLogger("TestDistributedLogReader-file-2");
    generateLogs(fileLogger, "Log message2", 0, 40);
    fileAppender.stop();

    generateCheckpointTime(LOGGING_CONTEXT_FILE, 40, cConf.get(Constants.Logging.KAFKA_TOPIC));

    // Generate logs for LOGGING_CONTEXT_KAFKA, logs only in kafka
    LoggingContextAccessor.setLoggingContext(LOGGING_CONTEXT_KAFKA);
    kafkaAppender = injector.getInstance(KafkaLogAppender.class);
    new LogAppenderInitializer(kafkaAppender).initialize("TestDistributedLogReader-kafka-3");
    kafkaLogger = LoggerFactory.getLogger("TestDistributedLogReader-kafka-3");
    generateLogs(kafkaLogger, "Log message3", 0, 30);
    kafkaAppender.stop();

  }

  @AfterClass
  public static void cleanUp() throws Exception {
    InMemoryTableService.reset();
    txManager.stopAndWait();
  }

  @Test
  public void testDistributedLogPrevBoth() throws Exception {
    ReadRange readRange = new ReadRange(0, Long.MAX_VALUE, LogOffset.INVALID_KAFKA_OFFSET);
    testDistributedLogPrev(readRange, LOGGING_CONTEXT_BOTH, 16, 4, "TestDistributedLogReader Log message1 ", 60);

    readRange = new ReadRange(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1),
                                        System.currentTimeMillis(), LogOffset.INVALID_KAFKA_OFFSET);
    testDistributedLogPrev(readRange, LOGGING_CONTEXT_BOTH, 16, 4, "TestDistributedLogReader Log message1 ", 60);

    testDistributedLogPrev(ReadRange.LATEST, LOGGING_CONTEXT_BOTH, 9, 8, "TestDistributedLogReader Log message1 ", 60);
  }

  @Test
  public void testDistributedLogNextBoth() throws Exception {
    ReadRange readRange = new ReadRange(0, Long.MAX_VALUE, LogOffset.INVALID_KAFKA_OFFSET);
    testDistributedLogNext(readRange, LOGGING_CONTEXT_BOTH, 20, 3, "TestDistributedLogReader Log message1 ", 60, 0);

    readRange = new ReadRange(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1),
                              System.currentTimeMillis(), LogOffset.INVALID_KAFKA_OFFSET);
    testDistributedLogNext(readRange, LOGGING_CONTEXT_BOTH, 20, 3, "TestDistributedLogReader Log message1 ", 60, 0);

    testDistributedLogNext(ReadRange.LATEST, LOGGING_CONTEXT_BOTH, 1, 3,
                           "TestDistributedLogReader Log message1 ", 3, 57);
  }

  @Test
  public void testDistributedLogPrevFile() throws Exception {
    ReadRange readRange = new ReadRange(0, Long.MAX_VALUE, LogOffset.INVALID_KAFKA_OFFSET);
    testDistributedLogPrev(readRange, LOGGING_CONTEXT_FILE, 7, 6, "TestDistributedLogReader Log message2 ", 40);

    readRange = new ReadRange(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1),
                                        System.currentTimeMillis(), LogOffset.INVALID_KAFKA_OFFSET);

    testDistributedLogPrev(readRange, LOGGING_CONTEXT_FILE, 7, 6, "TestDistributedLogReader Log message2 ", 40);

    testDistributedLogPrev(ReadRange.LATEST, LOGGING_CONTEXT_FILE, 7, 6, "TestDistributedLogReader Log message2 ", 40);
  }

  @Test
  public void testDistributedLogNextFile() throws Exception {
    ReadRange readRange = new ReadRange(0, Long.MAX_VALUE, LogOffset.INVALID_KAFKA_OFFSET);

    testDistributedLogNext(readRange, LOGGING_CONTEXT_FILE, 14, 3, "TestDistributedLogReader Log message2 ", 40, 0);

    readRange = new ReadRange(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1),
                              System.currentTimeMillis(), LogOffset.INVALID_KAFKA_OFFSET);
    testDistributedLogNext(readRange, LOGGING_CONTEXT_FILE, 14, 3, "TestDistributedLogReader Log message2 ", 40, 0);

    testDistributedLogNext(ReadRange.LATEST, LOGGING_CONTEXT_FILE, 1, 5,
                           "TestDistributedLogReader Log message2 ", 5, 35);
  }

  @Test
  public void testDistributedLogPrevKafka() throws Exception {
    ReadRange readRange = new ReadRange(0, Long.MAX_VALUE, LogOffset.INVALID_KAFKA_OFFSET);
    testDistributedLogPrev(readRange, LOGGING_CONTEXT_KAFKA, 5, 6, "TestDistributedLogReader Log message3 ", 30);

    readRange = new ReadRange(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1),
                                        System.currentTimeMillis(), LogOffset.INVALID_KAFKA_OFFSET);

    testDistributedLogPrev(readRange, LOGGING_CONTEXT_KAFKA, 5, 6, "TestDistributedLogReader Log message3 ", 30);

    testDistributedLogPrev(ReadRange.LATEST, LOGGING_CONTEXT_KAFKA, 5, 6, "TestDistributedLogReader Log message3 ", 30);
  }

  @Test
  public void testDistributedLogNextKafka() throws Exception {
    ReadRange readRange = new ReadRange(0, Long.MAX_VALUE, LogOffset.INVALID_KAFKA_OFFSET);
    testDistributedLogNext(readRange, LOGGING_CONTEXT_KAFKA, 10, 3, "TestDistributedLogReader Log message3 ", 30, 0);

    readRange = new ReadRange(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1),
                              System.currentTimeMillis(), LogOffset.INVALID_KAFKA_OFFSET);
    testDistributedLogNext(readRange, LOGGING_CONTEXT_KAFKA, 10, 3, "TestDistributedLogReader Log message3 ", 30, 0);

    testDistributedLogNext(ReadRange.LATEST, LOGGING_CONTEXT_KAFKA, 1, 8,
                           "TestDistributedLogReader Log message3 ", 8, 22);
  }

  private void testDistributedLogPrev(ReadRange readRange, LoggingContext loggingContext, int numCalls, int step,
                                      String assertMessage, int assertCount) throws Exception {
    DistributedLogReader distributedLogReader = injector.getInstance(DistributedLogReader.class);
    for (int i = 0; i < numCalls; ++i) {
      LoggingTester.LogCallback callback = new LoggingTester.LogCallback();
      distributedLogReader.getLogPrev(loggingContext, readRange, step, Filter.EMPTY_FILTER, callback);
      List<LogEvent> events = callback.getEvents();
      Assert.assertFalse(events.isEmpty());
      readRange = ReadRange.createToRange(events.get(0).getOffset());

      Collections.reverse(events);
      for (LogEvent event : events) {
        Assert.assertEquals(assertMessage + --assertCount,
                            event.getLoggingEvent().getFormattedMessage());
        // All logs generated by this test have value "system" in MDC property ".origin"
        Assert.assertEquals("system", event.getLoggingEvent().getMDCPropertyMap().get(".origin"));
      }
    }
    Assert.assertEquals(0, assertCount);
  }

  public void testDistributedLogNext(ReadRange readRange, LoggingContext loggingContext, int numCalls, int step,
                                     String assertMessage, int assertCount, int startIndex) throws Exception {
    DistributedLogReader distributedLogReader = injector.getInstance(DistributedLogReader.class);
    int count = 0;
    for (int i = 0; i < numCalls; ++i) {
      LoggingTester.LogCallback callback = new LoggingTester.LogCallback();
      distributedLogReader.getLogNext(loggingContext, readRange, step, Filter.EMPTY_FILTER, callback);
      List<LogEvent> events = callback.getEvents();
      Assert.assertFalse(events.isEmpty());
      readRange = ReadRange.createFromRange(events.get(events.size() - 1).getOffset());

      for (LogEvent event : events) {
        Assert.assertEquals(assertMessage + (startIndex + count++),
                            event.getLoggingEvent().getFormattedMessage());
        // All logs generated by this test have value "system" in MDC property ".origin"
        Assert.assertEquals("system", event.getLoggingEvent().getMDCPropertyMap().get(".origin"));
      }
    }
    Assert.assertEquals(assertCount, count);
  }

  private static void generateLogs(Logger logger, String baseMessage, int start, int max) throws InterruptedException {
    for (int i = start; i < start + max; ++i) {
      logger.warn("TestDistributedLogReader {} {}", baseMessage, i);
      TimeUnit.MILLISECONDS.sleep(1);
    }
  }

  private static void generateCheckpointTime(LoggingContext loggingContext,
                                             int numExpectedEvents, String kafkaTopic) throws IOException {
    FileLogReader logReader = injector.getInstance(FileLogReader.class);
    List<LogEvent> events =
      Lists.newArrayList(logReader.getLog(loggingContext, 0, Long.MAX_VALUE, Filter.EMPTY_FILTER));
    Assert.assertEquals(numExpectedEvents, events.size());

    // Save checkpoint (time of last event)
    TransactionRunner transactionRunner = injector.getInstance(TransactionRunner.class);
    CheckpointManager<KafkaOffset> checkpointManager = new KafkaCheckpointManager(transactionRunner,
                                      Constants.Logging.SYSTEM_PIPELINE_CHECKPOINT_PREFIX + kafkaTopic);

    long checkpointTime = events.get(numExpectedEvents - 1).getLoggingEvent().getTimeStamp();
    checkpointManager.saveCheckpoints(ImmutableMap.of(
      stringPartitioner.partition(loggingContext.getLogPartition(), -1),
      new Checkpoint<>(new KafkaOffset(numExpectedEvents, checkpointTime), checkpointTime)));
  }
}
