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

package co.cask.cdap.logging.appender.kafka;

import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.logging.KafkaTestBase;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.appender.LoggingTester;
import co.cask.cdap.logging.context.FlowletLoggingContext;
import co.cask.cdap.logging.read.KafkaLogReader;
import co.cask.cdap.test.SlowTests;
import org.apache.tephra.TransactionManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Test for logging.
 */
@Category(SlowTests.class)
public class TestKafkaLogging extends KafkaTestBase {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static TransactionManager txManager;

  @BeforeClass
  public static void init() throws Exception {
    txManager = KAFKA_TESTER.getInjector().getInstance(TransactionManager.class);
    txManager.startAndWait();

    LogAppender appender = KAFKA_TESTER.getInjector().getInstance(KafkaLogAppender.class);
    new LogAppenderInitializer(appender).initialize("TestKafkaLogging");

    Logger logger = LoggerFactory.getLogger("TestKafkaLogging");
    LoggingTester loggingTester = new LoggingTester();
    loggingTester.generateLogs(logger, new FlowletLoggingContext("TKL_NS_1", "APP_1", "FLOW_1", "FLOWLET_1",
                                                                 "RUN1", "INSTANCE1"));
    appender.stop();
  }

  @AfterClass
  public static void finish() {
    txManager.stopAndWait();
  }

  @Test
  public void testGetNext() throws Exception {
    // Check with null runId and null instanceId
    LoggingContext loggingContext = new FlowletLoggingContext("TKL_NS_1", "APP_1", "FLOW_1", "",
                                                              "RUN1", "INSTANCE1");
    KafkaLogReader logReader = KAFKA_TESTER.getInjector().getInstance(KafkaLogReader.class);
    LoggingTester tester = new LoggingTester();
    tester.testGetNext(logReader, loggingContext);
  }

  @Test
  public void testGetPrev() throws Exception {
    LoggingContext loggingContext = new FlowletLoggingContext("TKL_NS_1", "APP_1", "FLOW_1", "", "RUN1", "INSTANCE1");
    KafkaLogReader logReader = KAFKA_TESTER.getInjector().getInstance(KafkaLogReader.class);
    LoggingTester tester = new LoggingTester();
    tester.testGetPrev(logReader, loggingContext);
  }

  // Note: LogReader.getLog is tested in LogSaverTest for distributed mode
}
