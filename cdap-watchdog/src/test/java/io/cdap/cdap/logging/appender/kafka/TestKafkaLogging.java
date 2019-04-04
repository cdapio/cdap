/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.logging.appender.kafka;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.logging.KafkaTestBase;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.logging.appender.LoggingTester;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.logging.context.WorkerLoggingContext;
import io.cdap.cdap.logging.read.KafkaLogReader;
import io.cdap.cdap.logging.serialize.LoggingEventSerializer;
import io.cdap.cdap.test.SlowTests;
import org.apache.twill.kafka.client.FetchedMessage;
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
import java.util.Map;

/**
 * Test for logging to Kafka via {@link KafkaLogAppender}.
 */
@Category(SlowTests.class)
public class TestKafkaLogging extends KafkaTestBase {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void init() throws Exception {
    KafkaLogAppender appender = KAFKA_TESTER.getInjector().getInstance(KafkaLogAppender.class);
    new LogAppenderInitializer(appender).initialize("TestKafkaLogging");

    Logger logger = LoggerFactory.getLogger("TestKafkaLogging");
    LoggingTester loggingTester = new LoggingTester();
    loggingTester.generateLogs(logger, new WorkerLoggingContext("TKL_NS_1", "APP_1", "FLOW_1", "RUN1", "INSTANCE1"));
    appender.stop();
  }

  @Test
  public void testGetNext() throws Exception {
    // Check with null runId and null instanceId
    LoggingContext loggingContext = new WorkerLoggingContext("TKL_NS_1", "APP_1", "FLOW_1", "RUN1", "INSTANCE1");
    KafkaLogReader logReader = KAFKA_TESTER.getInjector().getInstance(KafkaLogReader.class);
    LoggingTester tester = new LoggingTester();
    tester.testGetNext(logReader, loggingContext);
  }

  @Test
  public void testGetPrev() throws Exception {
    LoggingContext loggingContext = new WorkerLoggingContext("TKL_NS_1", "APP_1", "FLOW_1", "RUN1", "INSTANCE1");
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
    LoggingContext loggingContext = new WorkerLoggingContext("TKL_NS_2", "APP_2", "FLOW_2", "RUN2", "INSTANCE2");
    LoggingContextAccessor.setLoggingContext(loggingContext);
    for (int i = 0; i < 40; ++i) {
      logger.warn("TKL_NS_2 Test log message {} {} {}", i, "arg1", "arg2", new Exception("test exception"));
    }

    loggingContext = new WorkerLoggingContext("TKL_NS_2", "APP_2", "FLOW_3", "RUN3", "INSTANCE3");
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
        // if we have already found another partition with application context, assert false
        Assert.assertFalse("Only one partition should have application logging context", isPresent);
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
    return Maps.immutableEntry(message.getTopicPartition().getPartition(), key);
  }
}
