/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.framework.distributed;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.kerberos.DefaultOwnerAdmin;
import co.cask.cdap.common.kerberos.OwnerAdmin;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data.runtime.TransactionExecutorModule;
import co.cask.cdap.kafka.KafkaTester;
import co.cask.cdap.logging.appender.LogMessage;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.framework.LogPathIdentifier;
import co.cask.cdap.logging.guice.DistributedLogFrameworkModule;
import co.cask.cdap.logging.meta.Checkpoint;
import co.cask.cdap.logging.meta.CheckpointManagerFactory;
import co.cask.cdap.logging.meta.FileMetaDataReader;
import co.cask.cdap.logging.read.LogEvent;
import co.cask.cdap.logging.serialize.LoggingEventSerializer;
import co.cask.cdap.logging.write.LogLocation;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.security.impersonation.CurrentUGIProvider;
import co.cask.cdap.security.impersonation.UGIProvider;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
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
import java.util.concurrent.Callable;
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
    Collections.<Module>emptyList(), 1
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
  }

  @After
  public void afterTest() throws Exception {
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
          createLoggingEvent("co.cask.test." + i, Level.INFO, "Testing " + i, eventTimeBase - i)
        )
      );
    }

    // Read the logs back. They should be sorted by timestamp order.
    final FileMetaDataReader metaDataReader = injector.getInstance(FileMetaDataReader.class);
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
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
      }
    }, 10, TimeUnit.SECONDS, msgCount, TimeUnit.MILLISECONDS);

    framework.stopAndWait();

    // Check the checkpoint is persisted correctly. Since all messages are processed,
    // the checkpoint should be the same as the message count.
    Checkpoint checkpoint = injector.getInstance(CheckpointManagerFactory.class)
      .create(cConf.get(Constants.Logging.KAFKA_TOPIC), Bytes.toBytes(100))
      .getCheckpoint(0);
    Assert.assertEquals(msgCount, checkpoint.getNextOffset());
  }

  private Injector createInjector() throws IOException {
    CConfiguration cConf = CConfiguration.copy(KAFKA_TESTER.getCConf());
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    // The event delay cannot be too small, otherwise the events will be out of order, especially on slow machine
    cConf.setLong(Constants.Logging.PIPELINE_EVENT_DELAY_MS, 2000);
    cConf.setLong("log.process.pipeline.checkpoint.interval.ms", 2000);

    return Guice.createInjector(
      new ConfigModule(cConf),
      new ZKClientModule(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new KafkaClientModule(),
      new LocationRuntimeModule().getInMemoryModules(),
      new DistributedLogFrameworkModule(new MockTwillContext()),
      new DataSetsModules().getInMemoryModules(),
      new TransactionModules().getInMemoryModules(),
      new TransactionExecutorModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new NamespaceClientRuntimeModule().getInMemoryModules(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getNoOpModule(),
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
