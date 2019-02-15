/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package co.cask.cdap.logging.pipeline.kafka;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.metrics.NoopMetricsContext;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.ServiceLoggingContext;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.StorageModule;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.kafka.KafkaTester;
import co.cask.cdap.logging.appender.LogMessage;
import co.cask.cdap.logging.context.GenericLoggingContext;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.context.WorkerLoggingContext;
import co.cask.cdap.logging.context.WorkflowProgramLoggingContext;
import co.cask.cdap.logging.framework.LocalAppenderContext;
import co.cask.cdap.logging.meta.Checkpoint;
import co.cask.cdap.logging.meta.CheckpointManager;
import co.cask.cdap.logging.meta.KafkaOffset;
import co.cask.cdap.logging.pipeline.LogPipelineConfigurator;
import co.cask.cdap.logging.pipeline.LogPipelineTestUtil;
import co.cask.cdap.logging.pipeline.LogProcessorPipelineContext;
import co.cask.cdap.logging.pipeline.MockAppender;
import co.cask.cdap.logging.serialize.LoggingEventSerializer;
import co.cask.cdap.metrics.collect.LocalMetricsCollectionService;
import co.cask.cdap.metrics.store.DefaultMetricStore;
import co.cask.cdap.metrics.store.LocalMetricsDatasetFactory;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.runtime.TransactionModules;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Unit-test for {@link KafkaLogProcessorPipeline}.
 */
public class KafkaLogProcessorPipelineTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private static final MetricsContext NO_OP_METRICS_CONTEXT = new NoopMetricsContext();

  @ClassRule
  public static final KafkaTester KAFKA_TESTER =
    new KafkaTester(ImmutableMap.of(),
                    ImmutableList.of(
                      new NonCustomLocationUnitTestModule(),
                      new DataSetsModules().getInMemoryModules(),
                      new TransactionModules().getInMemoryModules(),
                      new SystemDatasetRuntimeModule().getInMemoryModules(),
                      new AuthorizationTestModule(),
                      new AuthorizationEnforcementModule().getInMemoryModules(),
                      new AuthenticationContextModules().getNoOpModule(),
                      new StorageModule(),
                      new AbstractModule() {
                        @Override
                        protected void configure() {
                          bind(MetricsCollectionService.class).to(LocalMetricsCollectionService.class);
                          bind(MetricDatasetFactory.class).to(LocalMetricsDatasetFactory.class).in(Scopes.SINGLETON);
                          bind(MetricStore.class).to(DefaultMetricStore.class);
                        }
                      }),
                    1);

  @Test
  public void testBasicSort() throws Exception {
    String topic = "testPipeline";
    LoggerContext loggerContext = LogPipelineTestUtil.createLoggerContext("WARN",
                                                                          ImmutableMap.of("test.logger", "INFO"),
                                                                          MockAppender.class.getName());
    final MockAppender appender = LogPipelineTestUtil.getAppender(loggerContext.getLogger(Logger.ROOT_LOGGER_NAME),
                                                                  "Test", MockAppender.class);
    MockCheckpointManager checkpointManager = new MockCheckpointManager();
    KafkaPipelineConfig config = new KafkaPipelineConfig(topic, Collections.singleton(0), 1024L, 300L, 1048576, 500L);
    KAFKA_TESTER.createTopic(topic, 1);

    loggerContext.start();
    KafkaLogProcessorPipeline pipeline = new KafkaLogProcessorPipeline(
      new LogProcessorPipelineContext(CConfiguration.create(), "test", loggerContext, NO_OP_METRICS_CONTEXT, 0),
      checkpointManager,
      KAFKA_TESTER.getBrokerService(), config);

    pipeline.startAndWait();

    // Publish some log messages to Kafka
    long now = System.currentTimeMillis();
    publishLog(topic, ImmutableList.of(
      LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "0", now - 1000),
      LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "2", now - 700),
      LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "3", now - 500),
      LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "1", now - 900),
      LogPipelineTestUtil.createLoggingEvent("test.logger", Level.DEBUG, "hidden", now - 600),
      LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "4", now - 100))
    );

    // Since the messages are published in one batch, the processor should be able to fetch all of them,
    // hence the sorting order should be deterministic.
    // The DEBUG message should get filtered out
    Tasks.waitFor(5, () -> appender.getEvents().size(), 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    for (int i = 0; i < 5; i++) {
      Assert.assertEquals(Integer.toString(i), appender.getEvents().poll().getMessage());
    }

    // Now publish large messages that exceed the maximum queue size (1024). It should trigger writing regardless of
    // the event timestamp
    List<ILoggingEvent> events = new ArrayList<>(500);
    now = System.currentTimeMillis();
    for (int i = 0; i < 500; i++) {
      // The event timestamp is 10 seconds in future.
      events.add(LogPipelineTestUtil.createLoggingEvent("test.large.logger",
                                                        Level.WARN, "Large logger " + i, now + 10000));
    }
    publishLog(topic, events);

    Tasks.waitFor(true, () -> !appender.getEvents().isEmpty(), 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    events.clear();
    events.addAll(appender.getEvents());

    for (int i = 0; i < events.size(); i++) {
      Assert.assertEquals("Large logger " + i, events.get(i).getMessage());
    }

    pipeline.stopAndWait();
    loggerContext.stop();

    Assert.assertNull(appender.getEvents());
  }

  @Test
  public void testRegularFlush() throws Exception {
    String topic = "testFlush";
    LoggerContext loggerContext = LogPipelineTestUtil.createLoggerContext("WARN",
                                                                          ImmutableMap.of("test.logger", "INFO"),
                                                                          MockAppender.class.getName());
    final MockAppender appender = LogPipelineTestUtil.getAppender(loggerContext.getLogger(Logger.ROOT_LOGGER_NAME),
                                                                  "Test", MockAppender.class);
    MockCheckpointManager checkpointManager = new MockCheckpointManager();

    // Use a longer checkpoint time and a short event delay. Should expect flush called at least once
    // per event delay.
    KafkaPipelineConfig config = new KafkaPipelineConfig(topic, Collections.singleton(0), 1024, 100, 1048576, 2000);
    KAFKA_TESTER.createTopic(topic, 1);

    loggerContext.start();
    KafkaLogProcessorPipeline pipeline = new KafkaLogProcessorPipeline(
      new LogProcessorPipelineContext(CConfiguration.create(), "test", loggerContext, NO_OP_METRICS_CONTEXT, 0),
      checkpointManager,
      KAFKA_TESTER.getBrokerService(), config);

    pipeline.startAndWait();

    // Even when there is no event, the flush should still get called.
    Tasks.waitFor(5, appender::getFlushCount, 3, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    // Publish some logs
    long now = System.currentTimeMillis();
    publishLog(topic, ImmutableList.of(
      LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "0", now - 500),
      LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "1", now - 300),
      LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "2", now + 100)
    ));

    // Wait until getting all logs.
    Tasks.waitFor(3, () -> appender.getEvents().size(), 3, TimeUnit.SECONDS, 200, TimeUnit.MILLISECONDS);

    pipeline.stopAndWait();

    // Should get at least 20 flush calls, since the checkpoint is every 2 seconds
    Assert.assertTrue(appender.getFlushCount() >= 20);
  }

  @Test
  public void testMetricsAppender() throws Exception {
    Injector injector = KAFKA_TESTER.getInjector();
    MetricsCollectionService collectionService = injector.getInstance(MetricsCollectionService.class);
    collectionService.startAndWait();
    LoggerContext loggerContext = new LocalAppenderContext(injector.getInstance(DatasetFramework.class),
                                                           injector.getInstance(TransactionSystemClient.class),
                                                           injector.getInstance(LocationFactory.class),
                                                           injector.getInstance(MetricsCollectionService.class));
    final File logDir = TEMP_FOLDER.newFolder();
    loggerContext.putProperty("logDirectory", logDir.getAbsolutePath());

    LogPipelineConfigurator configurator = new LogPipelineConfigurator(CConfiguration.create());
    configurator.setContext(loggerContext);
    URL configURL = getClass().getClassLoader().getResource("pipeline-metric-appender.xml");
    Assert.assertNotNull(configURL);
    configurator.doConfigure(configURL);

    String topic = "metricsPipeline";
    MockCheckpointManager checkpointManager = new MockCheckpointManager();
    KafkaPipelineConfig config = new KafkaPipelineConfig(topic, Collections.singleton(0), 1024L, 100L, 1048576, 200L);
    KAFKA_TESTER.createTopic(topic, 1);

    loggerContext.start();
    KafkaLogProcessorPipeline pipeline = new KafkaLogProcessorPipeline(
      new LogProcessorPipelineContext(CConfiguration.create(), "testMetricAppender",
                                      loggerContext, NO_OP_METRICS_CONTEXT, 0),
      checkpointManager,
      KAFKA_TESTER.getBrokerService(), config);

    pipeline.startAndWait();

    // Publish some log messages to Kafka
    long now = System.currentTimeMillis();
    WorkerLoggingContext loggingContext =
      new WorkerLoggingContext("default", "app1", "worker1", "run1", "instance1");
    publishLog(topic,
               ImmutableList.of(
                 LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "0", now - 1000),
                 LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "2", now - 700),
                 LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "3", now - 500),
                 LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "1", now - 900),
                 LogPipelineTestUtil.createLoggingEvent("test.logger", Level.DEBUG, "hidden", now - 600),
                 LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "4", now - 100)),
               loggingContext
    );

    WorkflowProgramLoggingContext workflowProgramLoggingContext =
      new WorkflowProgramLoggingContext("default", "app1", "wflow1", "run1", ProgramType.MAPREDUCE, "mr1", "mrun1");

    publishLog(topic,
               ImmutableList.of(
                 LogPipelineTestUtil.createLoggingEvent("test.logger", Level.WARN, "0", now - 1000),
                 LogPipelineTestUtil.createLoggingEvent("test.logger", Level.WARN, "2", now - 700),
                 LogPipelineTestUtil.createLoggingEvent("test.logger", Level.TRACE, "3", now - 500)),
               workflowProgramLoggingContext
    );

    ServiceLoggingContext serviceLoggingContext =
      new ServiceLoggingContext(NamespaceId.SYSTEM.getNamespace(), Constants.Logging.COMPONENT_NAME,
                                Constants.Service.TRANSACTION);
    publishLog(topic,
               ImmutableList.of(
                 LogPipelineTestUtil.createLoggingEvent("test.logger", Level.ERROR, "0", now - 1000),
                 LogPipelineTestUtil.createLoggingEvent("test.logger", Level.ERROR, "2", now - 700),
                 LogPipelineTestUtil.createLoggingEvent("test.logger", Level.ERROR, "3", now - 500),
                 LogPipelineTestUtil.createLoggingEvent("test.logger", Level.INFO, "1", now - 900)),
               serviceLoggingContext
    );

    final MetricStore metricStore = injector.getInstance(MetricStore.class);
    try {
      verifyMetricsWithRetry(metricStore,
                             new MetricDataQuery(0, Integer.MAX_VALUE, Integer.MAX_VALUE,
                                                 "system.app.log.info",
                                                 AggregationFunction.SUM,
                                                 LoggingContextHelper.getMetricsTags(loggingContext),
                                                 new ArrayList<>()), 5L);

      verifyMetricsWithRetry(metricStore,
                             new MetricDataQuery(0, Integer.MAX_VALUE, Integer.MAX_VALUE,
                                                 "system.app.log.debug",
                                                 AggregationFunction.SUM,
                                                 LoggingContextHelper.getMetricsTags(loggingContext),
                                                 new ArrayList<>()), 1L);

      verifyMetricsWithRetry(metricStore,
                             new MetricDataQuery(0, Integer.MAX_VALUE, Integer.MAX_VALUE,
                                                 "system.app.log.warn",
                                                 AggregationFunction.SUM,
                                                 // mapreduce metrics context
                                                 ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, "default",
                                                                 Constants.Metrics.Tag.APP, "app1",
                                                                 Constants.Metrics.Tag.MAPREDUCE, "mr1",
                                                                 Constants.Metrics.Tag.RUN_ID, "mrun1"),
                                                 new ArrayList<>()), 2L);
      verifyMetricsWithRetry(metricStore,
                             new MetricDataQuery(0, Integer.MAX_VALUE, Integer.MAX_VALUE,
                                                 "system.app.log.trace",
                                                 AggregationFunction.SUM,
                                                 // workflow metrics context
                                                 ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, "default",
                                                                 Constants.Metrics.Tag.APP, "app1",
                                                                 Constants.Metrics.Tag.WORKFLOW, "wflow1",
                                                                 Constants.Metrics.Tag.RUN_ID, "run1"),
                                                 new ArrayList<>()), 1L);
      verifyMetricsWithRetry(metricStore,
                             new MetricDataQuery(0, Integer.MAX_VALUE, Integer.MAX_VALUE,
                                                 "system.services.log.error",
                                                 AggregationFunction.SUM,
                                                 LoggingContextHelper.getMetricsTags(serviceLoggingContext),
                                                 new ArrayList<>()), 3L);
    } finally {
      pipeline.stopAndWait();
      loggerContext.stop();
      collectionService.stopAndWait();
    }
  }

  private void verifyMetricsWithRetry(final MetricStore metricStore,
                                      final MetricDataQuery metricDataQuery, long expectedValue) throws Exception {
    Tasks.waitFor(expectedValue, () -> {
      try {
        return Iterables.getOnlyElement(
          Iterables.getOnlyElement(metricStore.query(metricDataQuery)).getTimeValues()).getValue();
      } catch (NoSuchElementException e) {
        return 0L;
      }
    }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testMultiAppenders() throws Exception {
    final File logDir = TEMP_FOLDER.newFolder();
    LoggerContext loggerContext = new LoggerContext();
    loggerContext.putProperty("logDirectory", logDir.getAbsolutePath());

    LogPipelineConfigurator configurator = new LogPipelineConfigurator(CConfiguration.create());
    configurator.setContext(loggerContext);
    URL configURL = getClass().getClassLoader().getResource("pipeline-multi-appenders.xml");
    Assert.assertNotNull(configURL);
    configurator.doConfigure(configURL);

    String topic = "testMultiAppenders";
    MockCheckpointManager checkpointManager = new MockCheckpointManager();
    KafkaPipelineConfig config = new KafkaPipelineConfig(topic, Collections.singleton(0), 1024L, 100L, 1048576, 200L);
    KAFKA_TESTER.createTopic(topic, 1);

    loggerContext.start();
    KafkaLogProcessorPipeline pipeline = new KafkaLogProcessorPipeline(
      new LogProcessorPipelineContext(CConfiguration.create(), "testMultiAppenders",
                                      loggerContext, NO_OP_METRICS_CONTEXT, 0),
      checkpointManager,
      KAFKA_TESTER.getBrokerService(), config);

    pipeline.startAndWait();

    // Publish some log messages to Kafka using a non-specific logger
    long now = System.currentTimeMillis();
    publishLog(topic, ImmutableList.of(
      LogPipelineTestUtil.createLoggingEvent("logger.trace", Level.TRACE, "TRACE", now - 1000),
      LogPipelineTestUtil.createLoggingEvent("logger.debug", Level.DEBUG, "DEBUG", now - 900),
      LogPipelineTestUtil.createLoggingEvent("logger.info", Level.INFO, "INFO", now - 800),
      LogPipelineTestUtil.createLoggingEvent("logger.warn", Level.WARN, "WARN", now - 700),
      LogPipelineTestUtil.createLoggingEvent("logger.error", Level.ERROR, "ERROR", now - 600)
    ));

    // All logs should get logged to the default.log file
    Tasks.waitFor(true, () -> {
      File logFile = new File(logDir, "default.log");
      List<String> lines = Files.readAllLines(logFile.toPath(), StandardCharsets.UTF_8);
      return Arrays.asList("TRACE", "DEBUG", "INFO", "WARN", "ERROR").equals(lines);
    }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    // Publish some more log messages via the non-additive "test.info" logger.
    now = System.currentTimeMillis();
    publishLog(topic, ImmutableList.of(
      LogPipelineTestUtil.createLoggingEvent("test.info.trace", Level.TRACE, "TRACE", now - 1000),
      LogPipelineTestUtil.createLoggingEvent("test.info.debug", Level.DEBUG, "DEBUG", now - 900),
      LogPipelineTestUtil.createLoggingEvent("test.info", Level.INFO, "INFO", now - 800),
      LogPipelineTestUtil.createLoggingEvent("test.info.warn", Level.WARN, "WARN", now - 700),
      LogPipelineTestUtil.createLoggingEvent("test.info.error", Level.ERROR, "ERROR", now - 600)
    ));

    // Only logs with INFO or above level should get written to the info.log file
    Tasks.waitFor(true, () -> {
      File logFile = new File(logDir, "info.log");
      List<String> lines = Files.readAllLines(logFile.toPath(), StandardCharsets.UTF_8);
      return Arrays.asList("INFO", "WARN", "ERROR").equals(lines);
    }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    // The default.log file shouldn't be changed, because the test.info logger is non additive
    File defaultLogFile = new File(logDir, "default.log");
    List<String> lines = Files.readAllLines(defaultLogFile.toPath(), StandardCharsets.UTF_8);
    Assert.assertEquals(Arrays.asList("TRACE", "DEBUG", "INFO", "WARN", "ERROR"), lines);

    // Publish a log messages via the additive "test.error" logger.
    now = System.currentTimeMillis();
    publishLog(topic, ImmutableList.of(
      LogPipelineTestUtil.createLoggingEvent("test.error.1.2", Level.ERROR, "ERROR", now - 1000)
    ));

    // Expect the log get appended to both the error.log file as well as the default.log file
    Tasks.waitFor(true, () -> {
      File logFile = new File(logDir, "error.log");
      List<String> lines1 = Files.readAllLines(logFile.toPath(), StandardCharsets.UTF_8);
      if (!Collections.singletonList("ERROR").equals(lines1)) {
        return false;
      }

      logFile = new File(logDir, "default.log");
      lines1 = Files.readAllLines(logFile.toPath(), StandardCharsets.UTF_8);
      return Arrays.asList("TRACE", "DEBUG", "INFO", "WARN", "ERROR", "ERROR").equals(lines1);
    }, 5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    pipeline.stopAndWait();
    loggerContext.stop();
  }

  /**
   * Publishes multiple log events.
   */
  private void publishLog(String topic, Iterable<ILoggingEvent> events) {
    publishLog(topic, events, new GenericLoggingContext(NamespaceId.DEFAULT.getNamespace(), "app", "entity"));
  }

  private void publishLog(String topic, Iterable<ILoggingEvent> events, LoggingContext context) {
    KafkaPublisher.Preparer preparer = KAFKA_TESTER.getKafkaClient()
      .getPublisher(KafkaPublisher.Ack.LEADER_RECEIVED, Compression.NONE)
      .prepare(topic);

    LoggingEventSerializer serializer = new LoggingEventSerializer();
    for (ILoggingEvent event : events) {
      preparer.add(ByteBuffer.wrap(serializer.toBytes(new LogMessage(event, context))), context.getLogPartition());
    }
    preparer.send();
  }

  /**
   * Checkpoint manager for unit tests.
   */
  class MockCheckpointManager implements CheckpointManager<KafkaOffset> {
    @Override
    public void saveCheckpoints(Map<Integer, ? extends Checkpoint<KafkaOffset>> checkpoints) throws Exception {

    }

    @Override
    public Map<Integer, Checkpoint<KafkaOffset>> getCheckpoint(Set<Integer> partitions) throws Exception {
      return Collections.emptyMap();
    }

    @Override
    public Checkpoint<KafkaOffset> getCheckpoint(int partition) throws Exception {
      return new Checkpoint<>(new KafkaOffset(-1, -1), -1);
    }
  }
}
