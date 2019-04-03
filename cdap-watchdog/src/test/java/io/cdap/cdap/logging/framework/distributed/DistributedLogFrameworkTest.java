/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

package io.cdap.cdap.logging.framework.distributed;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.KafkaClientModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.NamespaceAdminTestModule;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.ServiceLoggingContext;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.test.MockTwillContext;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data.runtime.TransactionExecutorModule;
import io.cdap.cdap.kafka.KafkaTester;
import io.cdap.cdap.logging.appender.LogMessage;
import io.cdap.cdap.logging.appender.system.LogPathIdentifier;
import io.cdap.cdap.logging.filter.Filter;
import io.cdap.cdap.logging.guice.DistributedLogFrameworkModule;
import io.cdap.cdap.logging.meta.Checkpoint;
import io.cdap.cdap.logging.meta.CheckpointManager;
import io.cdap.cdap.logging.meta.FileMetaDataReader;
import io.cdap.cdap.logging.meta.KafkaCheckpointManager;
import io.cdap.cdap.logging.meta.KafkaOffset;
import io.cdap.cdap.logging.read.LogEvent;
import io.cdap.cdap.logging.serialize.LoggingEventSerializer;
import io.cdap.cdap.logging.write.LogLocation;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.authorization.AuthorizationTestModule;
import io.cdap.cdap.security.impersonation.CurrentUGIProvider;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionModules;
import org.apache.twill.kafka.client.BrokerService;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Unit-test for {@link DistributedLogFramework}.
 */
public class DistributedLogFrameworkTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @ClassRule
  public static final KafkaTester KAFKA_TESTER = new KafkaTester(
    Collections.singletonMap(Constants.Logging.NUM_PARTITIONS, "1"),
    Collections.emptyList(), 1
  );

  private Injector injector;

  @BeforeClass
  public static void init() {
    CConfiguration cConf = KAFKA_TESTER.getCConf();
    KAFKA_TESTER.createTopic(cConf.get(Constants.Logging.KAFKA_TOPIC),
                             cConf.getInt(Constants.Logging.NUM_PARTITIONS));
  }

  @Before
  public void beforeTest() throws Exception {
    injector = createInjector();
    injector.getInstance(ZKClientService.class).startAndWait();
    injector.getInstance(KafkaClientService.class).startAndWait();
    injector.getInstance(BrokerService.class).startAndWait();
    injector.getInstance(TransactionManager.class).startAndWait();
    StructuredTableRegistry structuredTableRegistry = injector.getInstance(StructuredTableRegistry.class);
    structuredTableRegistry.initialize();
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class), structuredTableRegistry);
  }

  @After
  public void afterTest() {
    injector.getInstance(TransactionManager.class).stopAndWait();
    injector.getInstance(BrokerService.class).stopAndWait();
    injector.getInstance(KafkaClientService.class).stopAndWait();
    injector.getInstance(ZKClientService.class).stopAndWait();
    injector = null;
  }

  @Test
  public void testFramework() throws Exception {
    DistributedLogFramework framework = injector.getInstance(DistributedLogFramework.class);
    CConfiguration cConf = injector.getInstance(CConfiguration.class);

    framework.startAndWait();

    // Send some logs to Kafka.
    LoggingContext context = new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(),
                                                       Constants.Logging.COMPONENT_NAME,
                                                       "test");

    // Make sure all events get flushed in the same batch
    long eventTimeBase = System.currentTimeMillis() + cConf.getInt(Constants.Logging.PIPELINE_EVENT_DELAY_MS);
    final int msgCount = 50;
    for (int i = 0; i < msgCount; i++) {
      // Publish logs in descending timestamp order
      publishLog(
        cConf.get(Constants.Logging.KAFKA_TOPIC), context,
        ImmutableList.of(
          createLoggingEvent("io.cdap.test." + i, Level.INFO, "Testing " + i, eventTimeBase - i)
        )
      );
    }

    // Read the logs back. They should be sorted by timestamp order.
    final FileMetaDataReader metaDataReader = injector.getInstance(FileMetaDataReader.class);
    Tasks.waitFor(true, () -> {
      List<LogLocation> locations = metaDataReader.listFiles(new LogPathIdentifier(NamespaceId.SYSTEM.getNamespace(),
                                                                                    Constants.Logging.COMPONENT_NAME,
                                                                                   "test"), 0, Long.MAX_VALUE);
      if (locations.size() != 1) {
        return false;
      }
      LogLocation location = locations.get(0);
      int i = 0;
      try {
        try (CloseableIterator<LogEvent> iter = location.readLog(Filter.EMPTY_FILTER, 0, Long.MAX_VALUE, msgCount)) {
          while (iter.hasNext()) {
            String expectedMsg = "Testing " + (msgCount - i - 1);
            LogEvent event = iter.next();
            if (!expectedMsg.equals(event.getLoggingEvent().getMessage())) {
              return false;
            }
            i++;
          }
          return i == msgCount;
        }
      } catch (Exception e) {
        // It's possible the file is an invalid Avro file due to a race between creation of the meta data
        // and the time when actual content are flushed to the file
        return false;
      }
    }, 10, TimeUnit.SECONDS, msgCount, TimeUnit.MILLISECONDS);

    framework.stopAndWait();

    String kafkaTopic = cConf.get(Constants.Logging.KAFKA_TOPIC);
    // Check the checkpoint is persisted correctly. Since all messages are processed,
    // the checkpoint should be the same as the message count.
    CheckpointManager<KafkaOffset> checkpointManager = getCheckpointManager(kafkaTopic);
    Checkpoint<KafkaOffset> checkpoint = checkpointManager.getCheckpoint(0);
    Assert.assertEquals(msgCount, checkpoint.getOffset().getNextOffset());
  }

  private CheckpointManager<KafkaOffset> getCheckpointManager(String kafkaTopic) {
    TransactionRunner transactionRunner = injector.getInstance(TransactionRunner.class);
    return new KafkaCheckpointManager(transactionRunner,
                                      Constants.Logging.SYSTEM_PIPELINE_CHECKPOINT_PREFIX + kafkaTopic);
  }

  private Injector createInjector() throws IOException {
    CConfiguration cConf = CConfiguration.copy(KAFKA_TESTER.getCConf());
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    // The event delay cannot be too small, otherwise the events will be out of order, especially on slow machine
    cConf.setLong(Constants.Logging.PIPELINE_EVENT_DELAY_MS, 2000);
    cConf.setLong("log.process.pipeline.checkpoint.interval.ms", 2000);

    MockTwillContext mockTwillContext = new MockTwillContext();
    return Guice.createInjector(
      new ConfigModule(cConf),
      new ZKClientModule(),
      new InMemoryDiscoveryModule(),
      new KafkaClientModule(),
      new LocalLocationModule(),
      new DistributedLogFrameworkModule(mockTwillContext.getInstanceId(), mockTwillContext.getInstanceCount()),
      new DataSetsModules().getInMemoryModules(),
      new TransactionModules().getInMemoryModules(),
      new TransactionExecutorModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new NamespaceAdminTestModule(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getNoOpModule(),
      new StorageModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
          bind(UGIProvider.class).to(CurrentUGIProvider.class);
          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
        }
      }
    );
  }

  /**
   * Creates an {@link ILoggingEvent}.
   */
  private ILoggingEvent createLoggingEvent(String loggerName, Level level, String message, long timestamp) {
    LoggingEvent event = new LoggingEvent();
    event.setLevel(level);
    event.setLoggerName(loggerName);
    event.setMessage(message);
    event.setTimeStamp(timestamp);
    return event;
  }

  /**
   * Publishes multiple log events.
   */
  private void publishLog(String topic, LoggingContext context, Iterable<ILoggingEvent> events) {
    KafkaPublisher.Preparer preparer = KAFKA_TESTER.getKafkaClient()
      .getPublisher(KafkaPublisher.Ack.LEADER_RECEIVED, Compression.NONE)
      .prepare(topic);

    LoggingEventSerializer serializer = new LoggingEventSerializer();
    for (ILoggingEvent event : events) {
      preparer.add(ByteBuffer.wrap(serializer.toBytes(new LogMessage(event, context))), context.getLogPartition());
    }
    preparer.send();
  }

}
