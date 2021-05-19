/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.logging.appender.tms;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.logging.appender.LoggingTester;
import io.cdap.cdap.logging.appender.kafka.LogPartitionType;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.logging.context.MapReduceLoggingContext;
import io.cdap.cdap.logging.filter.Filter;
import io.cdap.cdap.logging.serialize.LoggingEventSerializer;
import io.cdap.cdap.messaging.MessageFetcher;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.data.RawMessage;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.test.SlowTests;
import org.apache.tephra.TxConstants;
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Test for logging to TMS via {@link TMSLogAppender}.
 */
@Category(SlowTests.class)
public class TestTMSLogging {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static CConfiguration cConf;
  private static MessagingService client;

  private static TMSLogAppender tmsLogAppender;
  private static Map<Integer, TopicId> topicIds;


  @BeforeClass
  public static void init() throws IOException, TopicAlreadyExistsException, UnauthorizedException {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setInt(Constants.MessagingSystem.HTTP_SERVER_CONSUME_CHUNK_SIZE, 128);
    // Set max life time to a high value so that dummy tx ids that we create in the tests still work
    cConf.setLong(TxConstants.Manager.CFG_TX_MAX_LIFETIME, 10000000000L);

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new InMemoryDiscoveryModule(),
      new MessagingServerRuntimeModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).toInstance(new NoOpMetricsCollectionService());
        }
      }
    );

    client = injector.getInstance(MessagingService.class);

    tmsLogAppender = injector.getInstance(TMSLogAppender.class);
    String topicPrefic = cConf.get(Constants.Logging.TMS_TOPIC_PREFIX);
    int numPartitions = cConf.getInt(Constants.Logging.NUM_PARTITIONS);
    topicIds = new HashMap<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      topicIds.put(i, NamespaceId.SYSTEM.topic(topicPrefic + i));
    }
    for (TopicId topicId : topicIds.values()) {
      client.createTopic(new TopicMetadata(topicId));
    }
  }

  @AfterClass
  public static void finish() throws TopicNotFoundException, IOException, UnauthorizedException {
    for (TopicId topicId : topicIds.values()) {
      client.deleteTopic(topicId);
    }
  }

  @Test
  public void testTmsLogAppender() throws Exception {
    // setup TMSLogAppender and log messages to it
    LogAppenderInitializer logAppenderInitializer = new LogAppenderInitializer(tmsLogAppender);
    logAppenderInitializer.initialize("TestTMSLogging");

    Logger logger = LoggerFactory.getLogger("TestTMSLogging");
    LoggingTester loggingTester = new LoggingTester();

    LoggingContext loggingContext = new MapReduceLoggingContext("TKL_NS_1", "APP_1", "MR_1", "RUN1");
    loggingTester.generateLogs(logger, loggingContext);

    logAppenderInitializer.close();

    // fetch and deserialize all the logs from TMS
    LoggingEventSerializer loggingEventSerializer = new LoggingEventSerializer();

    Map<Integer, List<ILoggingEvent>> partitionedFetchedLogs = new HashMap<>();
    int totalFetchedLogs = 0;

    for (Map.Entry<Integer, TopicId> topicId : topicIds.entrySet()) {
      List<ILoggingEvent> fetchedLogs = new ArrayList<>();
      MessageFetcher messageFetcher = client.prepareFetch(topicId.getValue());
      try (CloseableIterator<RawMessage> messages = messageFetcher.fetch()) {
        while (messages.hasNext()) {
          RawMessage message = messages.next();
          ILoggingEvent iLoggingEvent = loggingEventSerializer.fromBytes(ByteBuffer.wrap(message.getPayload()));
          fetchedLogs.add(iLoggingEvent);
        }
      }

      totalFetchedLogs += fetchedLogs.size();
      partitionedFetchedLogs.put(topicId.getKey(), fetchedLogs);
    }

    // LoggingTester emits 240 logs in total
    Assert.assertEquals(240, totalFetchedLogs);

    // Read the partition that our LoggingContext maps to and filter the logs in there to the logs that correspond
    // to our LoggingContext.
    LogPartitionType logPartitionType =
            LogPartitionType.valueOf(cConf.get(Constants.Logging.LOG_PUBLISH_PARTITION_KEY).toUpperCase());
    String partitionKey = logPartitionType.getPartitionKey(loggingContext);
    int partition = TMSLogAppender.partition(partitionKey, cConf.getInt(Constants.Logging.NUM_PARTITIONS));
    Filter logFilter = LoggingContextHelper.createFilter(loggingContext);

    List<ILoggingEvent> filteredLogs =
            partitionedFetchedLogs.get(partition).stream().filter(logFilter::match).collect(Collectors.toList());

    // LoggingTester emits 60 logs with the given LoggingContext
    Assert.assertEquals(60, filteredLogs.size());

    for (int i = 0; i < filteredLogs.size(); i++) {
      ILoggingEvent loggingEvent = filteredLogs.get(i);
      Assert.assertEquals(String.format("Test log message %s arg1 arg2", i), loggingEvent.getFormattedMessage());
    }
  }
}
