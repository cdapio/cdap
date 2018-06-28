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

package co.cask.cdap.logging.appender.tms;

import ch.qos.logback.classic.spi.ILoggingEvent;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.messaging.TopicAlreadyExistsException;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.appender.LoggingTester;
import co.cask.cdap.logging.appender.kafka.LogPartitionType;
import co.cask.cdap.logging.context.FlowletLoggingContext;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.serialize.LoggingEventSerializer;
import co.cask.cdap.messaging.MessageFetcher;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.data.RawMessage;
import co.cask.cdap.messaging.guice.MessagingServerRuntimeModule;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import co.cask.cdap.test.SlowTests;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
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
  public static void init() throws IOException, TopicAlreadyExistsException {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setInt(Constants.MessagingSystem.HTTP_SERVER_CONSUME_CHUNK_SIZE, 128);
    // Set max life time to a high value so that dummy tx ids that we create in the tests still work
    cConf.setLong(TxConstants.Manager.CFG_TX_MAX_LIFETIME, 10000000000L);

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new DiscoveryRuntimeModule().getInMemoryModules(),
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
  public static void finish() throws TopicNotFoundException, IOException {
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
    FlowletLoggingContext flowletLoggingContext = new FlowletLoggingContext("TKL_NS_1", "APP_1", "FLOW_1", "FLOWLET_1",
                                                                            "RUN1", "INSTANCE1");
    loggingTester.generateLogs(logger, flowletLoggingContext);

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

    // LoggingTester emits 220 logs in total
    Assert.assertEquals(220, totalFetchedLogs);

    // Read the partition that our LoggingContext maps to and filter the logs in there to the logs that correspond
    // to our LoggingContext.
    LogPartitionType logPartitionType =
            LogPartitionType.valueOf(cConf.get(Constants.Logging.LOG_PUBLISH_PARTITION_KEY).toUpperCase());
    String partitionKey = logPartitionType.getPartitionKey(flowletLoggingContext);
    int partition = TMSLogAppender.partition(partitionKey, cConf.getInt(Constants.Logging.NUM_PARTITIONS));
    Filter logFilter = LoggingContextHelper.createFilter(flowletLoggingContext);

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
