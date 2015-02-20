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

package co.cask.cdap.logging.appender.kafka;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.util.StatusPrinter;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.data2.datafabric.dataset.InMemoryDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryOrderedTableModule;
import co.cask.cdap.logging.KafkaTestBase;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.appender.LoggingTester;
import co.cask.cdap.logging.context.FlowletLoggingContext;
import co.cask.cdap.logging.read.DistributedLogReader;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.SlowTests;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.inmemory.InMemoryTxSystemClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * Kafka Test for logging.
 */
@Category(SlowTests.class)
public class TestKafkaLogging extends KafkaTestBase {
  private static InMemoryTxSystemClient txClient = null;
  private static DatasetFramework dsFramework;
  private static CConfiguration cConf;

  @BeforeClass
  public static void init() throws Exception {
    dsFramework = new InMemoryDatasetFramework(new InMemoryDefinitionRegistryFactory());
    dsFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE_ID, "table"),
                          new InMemoryOrderedTableModule());

    Configuration txConf = HBaseConfiguration.create();
    TransactionManager txManager = new TransactionManager(txConf);
    txManager.startAndWait();
    txClient = new InMemoryTxSystemClient(txManager);

    cConf = CConfiguration.create();
    cConf.set(LoggingConfiguration.KAFKA_SEED_BROKERS, "localhost:" + KafkaTestBase.getKafkaPort());
    cConf.set(LoggingConfiguration.NUM_PARTITIONS, "2");
    cConf.set(LoggingConfiguration.KAFKA_PRODUCER_TYPE, "sync");

    LogAppender appender = new KafkaLogAppender(cConf);
    new LogAppenderInitializer(appender).initialize("TestKafkaLogging");

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
    LoggingContext loggingContext = new FlowletLoggingContext("TFL_ACCT_1", "APP_1", "FLOW_1", "");
    DistributedLogReader logReader =
      new DistributedLogReader(dsFramework, txClient, cConf, new LocalLocationFactory());
    LoggingTester tester = new LoggingTester();
    tester.testGetNext(logReader, loggingContext);
    logReader.close();
  }

  @Test
  public void testGetPrev() throws Exception {
    LoggingContext loggingContext = new FlowletLoggingContext("TFL_ACCT_1", "APP_1", "FLOW_1", "");
    DistributedLogReader logReader =
      new DistributedLogReader(dsFramework, txClient, cConf, new LocalLocationFactory());
    LoggingTester tester = new LoggingTester();
    tester.testGetPrev(logReader, loggingContext);
    logReader.close();
  }
}
