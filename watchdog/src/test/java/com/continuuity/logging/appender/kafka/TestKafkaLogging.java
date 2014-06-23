package com.continuuity.logging.appender.kafka;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.common.logging.LoggingContextAccessor;
import com.continuuity.data.InMemoryDataSetAccessor;
import com.continuuity.data2.transaction.inmemory.InMemoryTransactionManager;
import com.continuuity.data2.transaction.inmemory.InMemoryTxSystemClient;
import com.continuuity.logging.KafkaTestBase;
import com.continuuity.logging.LoggingConfiguration;
import com.continuuity.logging.appender.LogAppender;
import com.continuuity.logging.appender.LogAppenderInitializer;
import com.continuuity.logging.appender.LoggingTester;
import com.continuuity.logging.context.FlowletLoggingContext;
import com.continuuity.logging.read.DistributedLogReader;
import com.continuuity.test.SlowTests;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Kafka Test for logging.
 */
@Category(SlowTests.class)
public class TestKafkaLogging extends KafkaTestBase {
  private static InMemoryTxSystemClient txClient = null;

  @BeforeClass
  public static void init() throws IOException {
    InMemoryTransactionManager txManager = new InMemoryTransactionManager();
    txManager.startAndWait();
    txClient = new InMemoryTxSystemClient(txManager);

    CConfiguration conf = CConfiguration.create();
    conf.set(LoggingConfiguration.KAFKA_SEED_BROKERS, "localhost:" + KafkaTestBase.getKafkaPort());
    conf.set(LoggingConfiguration.NUM_PARTITIONS, "2");
    conf.set(LoggingConfiguration.KAFKA_PRODUCER_TYPE, "sync");
    LogAppender appender = new LogAppenderInitializer(new KafkaLogAppender(conf)).initialize("TestKafkaLogging");

    Logger logger = LoggerFactory.getLogger("TestKafkaLogging");
    Exception e1 = new Exception("Test Exception1");
    Exception e2 = new Exception("Test Exception2", e1);

    LoggingContextAccessor.setLoggingContext(new FlowletLoggingContext("TFL_ACCT_2", "APP_1", "FLOW_1", "FLOWLET_1"));
    for (int i = 0; i < 40; ++i) {
      logger.warn("ACCT_2 Test log message " + i + " {} {}", "arg1", "arg2", e2);
    }

    LoggingContextAccessor.setLoggingContext(new FlowletLoggingContext("TFL_ACCT_1", "APP_1", "FLOW_1", "FLOWLET_1"));
    for (int i = 0; i < 20; ++i) {
      logger.warn("Test log message " + i + " {} {}", "arg1", "arg2", e2);
    }

    LoggingContextAccessor.setLoggingContext(new FlowletLoggingContext("TFL_ACCT_2", "APP_1", "FLOW_1", "FLOWLET_1"));
    for (int i = 40; i < 80; ++i) {
      logger.warn("ACCT_2 Test log message " + i + " {} {}", "arg1", "arg2", e2);
    }

    LoggingContextAccessor.setLoggingContext(new FlowletLoggingContext("TFL_ACCT_1", "APP_1", "FLOW_1", "FLOWLET_1"));
    for (int i = 20; i < 40; ++i) {
      logger.warn("Test log message " + i + " {} {}", "arg1", "arg2", e2);
    }

    LoggingContextAccessor.setLoggingContext(new FlowletLoggingContext("TFL_ACCT_1", "APP_1", "FLOW_1", "FLOWLET_1"));
    for (int i = 40; i < 60; ++i) {
      logger.warn("Test log message " + i + " {} {}", "arg1", "arg2", e2);
    }

    LoggingContextAccessor.setLoggingContext(new FlowletLoggingContext("TFL_ACCT_2", "APP_1", "FLOW_1", "FLOWLET_1"));
    for (int i = 80; i < 120; ++i) {
      logger.warn("ACCT_2 Test log message " + i + " {} {}", "arg1", "arg2", e2);
    }

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    StatusPrinter.setPrintStream(new PrintStream(bos));
    StatusPrinter.print((LoggerContext) LoggerFactory.getILoggerFactory());
    System.out.println(bos.toString());

    appender.stop();
  }

  @Test
  public void testGetNext() throws Exception {
    CConfiguration conf = new CConfiguration();
    conf.set(LoggingConfiguration.KAFKA_SEED_BROKERS, "localhost:" + KafkaTestBase.getKafkaPort());
    conf.set(LoggingConfiguration.NUM_PARTITIONS, "2");
    conf.set(LoggingConfiguration.LOG_RUN_ACCOUNT, "TFL_ACCT_1");

    LoggingContext loggingContext = new FlowletLoggingContext("TFL_ACCT_1", "APP_1", "FLOW_1", "");
    DistributedLogReader logReader =
      new DistributedLogReader(new InMemoryDataSetAccessor(conf), txClient, conf, new LocalLocationFactory());
    LoggingTester tester = new LoggingTester();
    tester.testGetNext(logReader, loggingContext);
    logReader.close();
  }

  @Test
  public void testGetPrev() throws Exception {
    CConfiguration conf = new CConfiguration();
    conf.set(LoggingConfiguration.KAFKA_SEED_BROKERS, "localhost:" + KafkaTestBase.getKafkaPort());
    conf.set(LoggingConfiguration.NUM_PARTITIONS, "2");
    conf.set(LoggingConfiguration.LOG_RUN_ACCOUNT, "TFL_ACCT_1");

    LoggingContext loggingContext = new FlowletLoggingContext("TFL_ACCT_1", "APP_1", "FLOW_1", "");
    DistributedLogReader logReader =
      new DistributedLogReader(new InMemoryDataSetAccessor(conf), txClient, conf, new LocalLocationFactory());
    LoggingTester tester = new LoggingTester();
    tester.testGetPrev(logReader, loggingContext);
    logReader.close();
  }
}
