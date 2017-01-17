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

package co.cask.cdap.metrics.process;

import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.MetricValues;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.metrics.NoopMetricsContext;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.io.BinaryEncoder;
import co.cask.cdap.common.io.Encoder;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.common.security.UGIProvider;
import co.cask.cdap.common.security.UnsupportedUGIProvider;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.internal.io.DatumReaderFactory;
import co.cask.cdap.internal.io.SchemaGenerator;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.metrics.MessagingMetricsTestBase;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.apache.tephra.TransactionManager;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.internal.utils.Networks;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Testing the basic properties of the {@link MessagingMetricsProcessorService}.
 */
public class MessagingMetricsProcessorServiceTest extends MessagingMetricsTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(MessagingMetricsProcessorServiceTest.class);

  @ClassRule
  public static TemporaryFolder tmpFolder1 = new TemporaryFolder();
  @ClassRule
  public static TemporaryFolder tmpFolder2 = new TemporaryFolder();

  private InMemoryZKServer zkServer;
  private EmbeddedKafkaServer kafkaServer;

  private static final int PARTITION_SIZE = 2;
  private static final long START_TIME = System.currentTimeMillis() / 1000;

  private ByteArrayOutputStream encoderOutputStream = new ByteArrayOutputStream(1024);
  private Encoder encoder = new BinaryEncoder(encoderOutputStream);

  @Test
  public void testMetricsProcessor() throws Exception {
    injector.getInstance(TransactionManager.class).startAndWait();
    injector.getInstance(DatasetOpExecutor.class).startAndWait();
    injector.getInstance(DatasetService.class).startAndWait();
    zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    Properties kafkaConfig = generateKafkaConfig(tmpFolder1);
    kafkaServer = new EmbeddedKafkaServer(kafkaConfig);
    kafkaServer.startAndWait();
    ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();
    KafkaClientService kafkaClient = new ZKKafkaClientService(zkClient);
    kafkaClient.startAndWait();

    MetricStore metricStore = injector.getInstance(MetricStore.class);

    Set<Integer> partitions = new HashSet<>();
    for (int i = 0; i < PARTITION_SIZE; i++) {
      partitions.add(i);
    }

    final Map<String, String> metricsContext = new HashMap<>();
    metricsContext.put(Constants.Metrics.Tag.NAMESPACE, "NS_1");
    metricsContext.put(Constants.Metrics.Tag.APP, "APP_1");
    metricsContext.put(Constants.Metrics.Tag.FLOW, "FLOW_1");
    MetricsContext context = new NoopMetricsContext(metricsContext);

    KafkaMetricsProcessorService kafkaMetricsProcessorService =
      new KafkaMetricsProcessorService(kafkaClient,
                                       injector.getInstance(MetricDatasetFactory.class),
                                       new MetricsMessageCallbackFactory(injector.getInstance(SchemaGenerator.class),
                                                                         injector.getInstance(DatumReaderFactory.class),
                                                                         metricStore, 4), topicPrefix, partitions);

    kafkaMetricsProcessorService.setMetricsContext(context);
    kafkaMetricsProcessorService.startAndWait();

    KafkaPublisher publisher = kafkaClient.getPublisher(KafkaPublisher.Ack.FIRE_AND_FORGET, Compression.SNAPPY);
    KafkaPublisher.Preparer preparer = publisher.prepare(topicPrefix);

    Map<String, Long> expected = new HashMap<>();
    // Publish metrics to Kafka and record expected metrics before kafkaMetricsProcessorService starts
    for (int i = 1; i < 10; i++) {
      addKafkaMetrics(i, metricsContext, expected, preparer);
    }
    preparer.send();



    // Intentionally set fetcher persist threshold to a small value, so that MessagingMetricsProcessorService
    // internally can persist metrics when more messages are to be fetched
    MessagingMetricsProcessorService messagingMetricsProcessorService =
      new MessagingMetricsProcessorService(injector.getInstance(MetricDatasetFactory.class), topicPrefix,
                                           partitions, messagingService, injector.getInstance(SchemaGenerator.class),
                                           injector.getInstance(DatumReaderFactory.class),
                                           metricStore,
                                           1);



    messagingMetricsProcessorService.setMetricsContext(context);
    messagingMetricsProcessorService.startAndWait();

    // Publish metrics and record expected metrics
    for (int i = 10; i < 20; i++) {
      publishMessagingMetrics(i, metricsContext, expected);
    }

    Thread.sleep(5000);
    // Stop and restart messagingMetricsProcessorService
    messagingMetricsProcessorService.stopAndWait();
    // Intentionally set fetcher persist threshold to a large value, so that MessagingMetricsProcessorService
    // internally only persists metrics during terminating.
    messagingMetricsProcessorService =
      new MessagingMetricsProcessorService(injector.getInstance(MetricDatasetFactory.class), topicPrefix,
                                           partitions, messagingService, injector.getInstance(SchemaGenerator.class),
                                           injector.getInstance(DatumReaderFactory.class),
                                           metricStore,
                                           100);
    messagingMetricsProcessorService.startAndWait();

    for (int i = 20; i < 30; i++) {
      publishMessagingMetrics(i, metricsContext, expected);
    }

    Thread.sleep(5000);

    // Query metrics from the metricStore and compare them with the expected ones
    assertMetricsResult(metricStore, metricsContext, expected);

    messagingMetricsProcessorService.stopAndWait();
    kafkaServer.stopAndWait();
    zkServer.stopAndWait();
  }

  private void addKafkaMetrics(int i, Map<String, String> metricsContext, Map<String, Long> expected,
                                   KafkaPublisher.Preparer preparer)
    throws IOException, TopicNotFoundException {
    MetricValues metric = getExpectedMetricValues(i, metricsContext, expected);
    // Encode each MetricRecord into bytes and make it an individual kafka message in a message set.
    recordWriter.encode(metric, encoder);
    // partitioning by the context
    preparer.add(ByteBuffer.wrap(encoderOutputStream.toByteArray()), metric.getTags().hashCode());
    encoderOutputStream.reset();
    LOG.info("Published metric: {}", metric);
    encoderOutputStream.reset();
  }

  private void publishMessagingMetrics(int i, Map<String, String> metricsContext, Map<String, Long> expected)
    throws IOException, TopicNotFoundException {
    MetricValues metric = getExpectedMetricValues(i, metricsContext, expected);
    messagingService.publish(StoreRequestBuilder.of(metricsTopics[i % PARTITION_SIZE])
                               .addPayloads(encoderOutputStream.toByteArray()).build());
    LOG.info("Published metric: {}", metric);
    encoderOutputStream.reset();
  }

  private MetricValues getExpectedMetricValues(int i, Map<String, String> metricsContext, Map<String, Long> expected)
    throws TopicNotFoundException, IOException {
    String metricName = "processed" + "_" + i;
    MetricValues metric =
      new MetricValues(metricsContext, metricName, START_TIME + i * 100, i, MetricType.GAUGE);
    recordWriter.encode(metric, encoder);
    expected.put("system." + metricName, (long) i);
    return metric;
  }

  private void assertMetricsResult(MetricStore metricStore, Map<String, String> metricsContext,
                                   Map<String, Long> expected) {
    for (Map.Entry<String, Long> metric : expected.entrySet()) {
      MetricDataQuery metricDataQuery =
        new MetricDataQuery(0, Integer.MAX_VALUE, Integer.MAX_VALUE, metric.getKey(),
                            AggregationFunction.SUM,
                            metricsContext,
                            ImmutableList.<String>of());
      Collection<MetricTimeSeries> query = metricStore.query(metricDataQuery);
      MetricTimeSeries timeSeries = Iterables.getOnlyElement(query);
      List<TimeValue> timeValues = timeSeries.getTimeValues();
      TimeValue timeValue = Iterables.getOnlyElement(timeValues);
      Assert.assertEquals(metric.getValue().longValue(), timeValue.getValue());
    }
  }

  @Override
  protected List<Module> getAdditionalModules() {
    List<Module> list = new ArrayList<>();
    list.add(new DataSetsModules().getStandaloneModules());
    list.add(new IOModule());
    list.add(Modules.override(
      new NonCustomLocationUnitTestModule().getModule(),
      new DataFabricModules().getInMemoryModules(),
      new DataSetServiceModules().getInMemoryModules(),
      new ExploreClientModule(),
      new NamespaceClientRuntimeModule().getInMemoryModules(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule()
    ).with(new AbstractModule() {
      @Override
      protected void configure() {
        bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
      }
    }));
    return list;
  }


  private Properties generateKafkaConfig(TemporaryFolder tmpFolder) throws IOException {
    int port = Networks.getRandomPort();
    Preconditions.checkState(port > 0, "Failed to get random port.");

    Properties prop = new Properties();
    prop.setProperty("broker.id", "1");
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("num.network.threads", "2");
    prop.setProperty("num.io.threads", "2");
    prop.setProperty("socket.send.buffer.bytes", "1048576");
    prop.setProperty("socket.receive.buffer.bytes", "1048576");
    prop.setProperty("socket.request.max.bytes", "104857600");
    prop.setProperty("log.dir", tmpFolder.newFolder().getAbsolutePath());
    prop.setProperty("num.partitions", "1");
    prop.setProperty("log.flush.interval.messages", "10000");
    prop.setProperty("log.flush.interval.ms", "1000");
    prop.setProperty("log.retention.hours", "1");
    prop.setProperty("log.segment.bytes", "536870912");
    prop.setProperty("log.cleanup.interval.mins", "1");
    prop.setProperty("zookeeper.connect", zkServer.getConnectionStr());
    prop.setProperty("zookeeper.connection.timeout.ms", "1000000");

    return prop;
  }
}
