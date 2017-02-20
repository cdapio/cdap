/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

import ch.qos.logback.classic.spi.ILoggingEvent;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.logging.KafkaTestBase;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.appender.LoggingTester;
import co.cask.cdap.logging.context.FlowletLoggingContext;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.read.KafkaLogReader;
import co.cask.cdap.test.SlowTests;
import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.apache.tephra.TransactionManager;
import org.apache.twill.kafka.client.FetchedMessage;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Test for logging.
 */
@Category(SlowTests.class)
public class TestKafkaLogging extends KafkaTestBase {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static TransactionManager txManager;
  private static LogAppender appender;

  @BeforeClass
  public static void init() throws Exception {
    txManager = KAFKA_TESTER.getInjector().getInstance(TransactionManager.class);
    txManager.startAndWait();

    appender = KAFKA_TESTER.getInjector().getInstance(KafkaLogAppender.class);
    new LogAppenderInitializer(appender).initialize("TestKafkaLogging");

    Logger logger = LoggerFactory.getLogger("TestKafkaLogging");
    LoggingTester loggingTester = new LoggingTester();
    loggingTester.generateLogs(logger, new FlowletLoggingContext("TKL_NS_1", "APP_1", "FLOW_1", "FLOWLET_1",
                                                                 "RUN1", "INSTANCE1"));
  }

  @AfterClass
  public static void finish() {
    txManager.stopAndWait();
    appender.stop();
  }

  @Test
  public void testGetNext() throws Exception {
    // Check with null runId and null instanceId
    LoggingContext loggingContext = new FlowletLoggingContext("TKL_NS_1", "APP_1", "FLOW_1", "", "RUN1", "INSTANCE1");
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

  @Test
  public void testPartitionKey() throws Exception {
    CConfiguration cConf = KAFKA_TESTER.getCConf();
    // set kafka partition key to application
    cConf.set(Constants.Logging.LOG_PUBLISH_PARTITION_KEY, "application");

    Logger logger = LoggerFactory.getLogger("TestKafkaLogging");
    LoggingContext loggingContext = new FlowletLoggingContext("TKL_NS_2", "APP_2", "FLOW_2", "FLOWLET_2",
                                                              "RUN2", "INSTANCE2");
    LoggingContextAccessor.setLoggingContext(loggingContext);
    for (int i = 0; i < 40; ++i) {
      logger.warn("TKL_NS_2 Test log message {} {} {}", i, "arg1", "arg2", new Exception("test exception"));
    }

    loggingContext = new FlowletLoggingContext("TKL_NS_2", "APP_2", "FLOW_3", "FLOWLET_3",
                                                              "RUN3", "INSTANCE3");
    LoggingContextAccessor.setLoggingContext(loggingContext);
    for (int i = 0; i < 40; ++i) {
      logger.warn("TKL_NS_2 Test log message {} {} {}", i, "arg1", "arg2", new Exception("test exception"));
    }

    final Multimap<Integer, String> actual = ArrayListMultimap.create();

    KAFKA_TESTER.getPublishedMessages(KAFKA_TESTER.getCConf().get(Constants.Logging.KAFKA_TOPIC),
                                      ImmutableSet.of(0, 1), 40, new Function<FetchedMessage, String>() {
        @Override
        public String apply(final FetchedMessage input) {
          try {
            Map.Entry<Integer, String> entry = convertFetchedMessage(input);
            actual.put(entry.getKey(), entry.getValue());
          } catch (IOException e) {
            // should never happen
          }
          return "";
        }
      });

    boolean isPresent = false;

    // check if all the logs from same app went to same partition
    for (Map.Entry<Integer, Collection<String>> entry : actual.asMap().entrySet()) {
      if (entry.getValue().contains("TKL_NS_2:APP_2")) {
        if (isPresent) {
          // if we have already found another partition with application context, assert false
          Assert.assertFalse("Only one partition should have application logging context", isPresent);
        }
        isPresent = true;
      }
    }

    // reset kafka partition key
    cConf.set(Constants.Logging.LOG_PUBLISH_PARTITION_KEY, "program");
  }

  private Map.Entry<Integer, String> convertFetchedMessage(FetchedMessage message) throws IOException {
    LoggingEventSerializer serializer = new LoggingEventSerializer();
    ILoggingEvent iLoggingEvent = serializer.fromBytes(message.getPayload());
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContext(iLoggingEvent.getMDCPropertyMap());
    String key = loggingContext.getLogPartition();

    // Temporary map for pretty format
    Map<String, String> tempMap = new HashMap<>();
    tempMap.put("Timestamp", Long.toString(iLoggingEvent.getTimeStamp()));
    return Maps.immutableEntry(message.getTopicPartition().getPartition(), key);
  }
}
