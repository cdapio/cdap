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
import co.cask.cdap.api.metrics.NoopMetricsContext;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.internal.io.DatumReaderFactory;
import co.cask.cdap.internal.io.SchemaGenerator;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Testing the basic properties of the {@link MessagingMetricsProcessorService}
 * and {@link KafkaMetricsProcessorService}.
 */
public class MetricsProcessorServiceTest extends MetricsProcessorServiceTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsProcessorServiceTest.class);

  private static final String SYSTEM_METRIC_PREFIX = "system.";

  @ClassRule
  public static TemporaryFolder tmpFolder1 = new TemporaryFolder();

  private InMemoryZKServer zkServer;

  @Test
  public void testMetricsProcessor() throws Exception {
    injector.getInstance(TransactionManager.class).startAndWait();
    injector.getInstance(DatasetOpExecutor.class).startAndWait();
    injector.getInstance(DatasetService.class).startAndWait();
    zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    Properties kafkaConfig = generateKafkaConfig(tmpFolder1);
    EmbeddedKafkaServer kafkaServer = new EmbeddedKafkaServer(kafkaConfig);
    kafkaServer.startAndWait();
    ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();
    KafkaClientService kafkaClient = new ZKKafkaClientService(zkClient);
    kafkaClient.startAndWait();

    final MetricStore metricStore = injector.getInstance(MetricStore.class);

    Set<Integer> partitions = new HashSet<>();
    for (int i = 0; i < PARTITION_SIZE; i++) {
      partitions.add(i);
    }

    KafkaPublisher publisher = kafkaClient.getPublisher(KafkaPublisher.Ack.FIRE_AND_FORGET, Compression.SNAPPY);
    final KafkaPublisher.Preparer preparer = publisher.prepare(TOPIC_PREFIX);

    // Wait for metrics to be successfully published to Kafka. Retry if publishing fails.
    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return publishKafkaMetrics(METRICS_CONTEXT, expected, preparer);
      }
    }, 15, TimeUnit.SECONDS, "Failed to publish correct number of metrics to Kafka");

    // Start KafkaMetricsProcessorService after metrics are published to Kafka
    KafkaMetricsProcessorService kafkaMetricsProcessorService =
      new KafkaMetricsProcessorService(kafkaClient,
                                       injector.getInstance(MetricDatasetFactory.class),
                                       new MetricsMessageCallbackFactory(injector.getInstance(SchemaGenerator.class),
                                                                         injector.getInstance(DatumReaderFactory.class),
                                                                         metricStore, 4), TOPIC_PREFIX, partitions,
                                       new NoopMetricsContext());
    kafkaMetricsProcessorService.startAndWait();

    // Intentionally set queue size to a small value, so that MessagingMetricsProcessorService
    // internally can persist metrics when more messages are to be fetched
    MessagingMetricsProcessorService messagingMetricsProcessorService =
      new MessagingMetricsProcessorService(injector.getInstance(MetricDatasetFactory.class), TOPIC_PREFIX,
                                           messagingService, injector.getInstance(SchemaGenerator.class),
                                           injector.getInstance(DatumReaderFactory.class),
                                           metricStore, 1000L, 5, partitions, new NoopMetricsContext(), 50);
    messagingMetricsProcessorService.startAndWait();

    // Publish metrics with messaging service and record expected metrics
    for (int i = 10; i < 20; i++) {
      publishMessagingMetrics(i, METRICS_CONTEXT, expected, SYSTEM_METRIC_PREFIX, MetricType.COUNTER);
    }

    Thread.sleep(500);
    // Stop and restart messagingMetricsProcessorService
    messagingMetricsProcessorService.stopAndWait();
    // Intentionally set queue size to a large value, so that MessagingMetricsProcessorService
    // internally only persists metrics during terminating.
    messagingMetricsProcessorService =
      new MessagingMetricsProcessorService(injector.getInstance(MetricDatasetFactory.class), TOPIC_PREFIX,
                                           messagingService, injector.getInstance(SchemaGenerator.class),
                                           injector.getInstance(DatumReaderFactory.class),
                                           metricStore, 1000L, 100, partitions, new NoopMetricsContext(), 50);
    messagingMetricsProcessorService.startAndWait();

    // Publish metrics after MessagingMetricsProcessorService restarts and record expected metrics
    for (int i = 20; i < 30; i++) {
      publishMessagingMetrics(i, METRICS_CONTEXT, expected, SYSTEM_METRIC_PREFIX, MetricType.GAUGE);
    }

    final List<String> missingMetricNames = new ArrayList<>();
    // Wait until all expected metrics can be queried from the metric store. If not all expected metrics
    // are retrieved when timeout occurs, print out the missing metrics
    try {
      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return canQueryAllMetrics(metricStore, METRICS_CONTEXT, expected, missingMetricNames);
        }
      }, 10000, TimeUnit.MILLISECONDS, "Failed to get all metrics");
    } catch (TimeoutException e) {
      Assert.fail(String.format("Metrics: [%s] cannot be found in the metrics store.",
                                Joiner.on(", ").join(missingMetricNames)));
    }

    // Query metrics from the metricStore and compare them with the expected ones
    assertMetricsResult(metricStore, METRICS_CONTEXT, expected);

    // Query for the 5 counter metrics published with messaging between time 5 - 14
    Collection<MetricTimeSeries> queryResult =
      metricStore.query(new MetricDataQuery(5, 14, 1, Integer.MAX_VALUE,
                                            ImmutableMap.of(SYSTEM_METRIC_PREFIX + COUNTER_METRIC_NAME,
                                                            AggregationFunction.SUM),
                                            METRICS_CONTEXT, ImmutableList.<String>of(), null));
    MetricTimeSeries timeSeries = Iterables.getOnlyElement(queryResult);
    Assert.assertEquals(5, timeSeries.getTimeValues().size());
    for (TimeValue timeValue : timeSeries.getTimeValues()) {
      Assert.assertEquals(1L, timeValue.getValue());
    }

    // Stop services and servers
    kafkaMetricsProcessorService.stopAndWait();
    messagingMetricsProcessorService.stopAndWait();
    kafkaServer.stopAndWait();
    zkServer.stopAndWait();
    // Delete all metrics
    metricStore.deleteAll();
  }

  /**
   * Publish metrics to Kafka. Return {@code true} if correct number of messages are published,
   * otherwise return {@code false}
   */
  private boolean publishKafkaMetrics(Map<String, String> metricsContext, Map<String, Long> expected,
                                      KafkaPublisher.Preparer preparer) {
    // Publish metrics to Kafka and record expected metrics before kafkaMetricsProcessorService starts
    for (int metricIndex = 0; metricIndex < 10; metricIndex++) {
      try {
        addKafkaMetrics(metricIndex, metricsContext, expected, preparer, MetricType.GAUGE);
      } catch (Exception e) {
        LOG.error("Failed to add metric with index {} to Kafka", metricIndex, e);
      }
    }
    int publishedMetricsCount = 0;
    try {
      publishedMetricsCount = preparer.send().get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.error("Exception occurs when sending metrics to Kafka", e);
    }
    return publishedMetricsCount == 10;
  }

  private void addKafkaMetrics(int metricIndex, Map<String, String> metricsContext, Map<String, Long> expected,
                               KafkaPublisher.Preparer preparer, MetricType metricType)
    throws IOException, TopicNotFoundException {
    MetricValues metric = getMetricValuesAddToExpected(metricIndex, metricsContext, expected,
                                                       SYSTEM_METRIC_PREFIX, metricType);
    // partitioning by the context
    preparer.add(ByteBuffer.wrap(encoderOutputStream.toByteArray()), metric.getTags().hashCode());
    encoderOutputStream.reset();
    LOG.info("Published metric: {}", metric);
    encoderOutputStream.reset();
  }

  /**
   * Checks whether all expected metrics can be obtained with query
   */
  private boolean canQueryAllMetrics(MetricStore metricStore, Map<String, String> metricsContext,
                                     Map<String, Long> expected, List<String> missingMetricNames) {
    missingMetricNames.clear();
    for (Map.Entry<String, Long> metric : expected.entrySet()) {
      Collection<MetricTimeSeries> queryResult =
        metricStore.query(new MetricDataQuery(0, Integer.MAX_VALUE, Integer.MAX_VALUE,
                                              metric.getKey(), AggregationFunction.SUM,
                                              metricsContext, ImmutableList.<String>of()));
      if (queryResult.size() == 0) {
        missingMetricNames.add(metric.getKey());
      }
    }
    return missingMetricNames.isEmpty();
  }

  private void assertMetricsResult(MetricStore metricStore, Map<String, String> metricsContext,
                                   Map<String, Long> expected) {
    for (Map.Entry<String, Long> metric : expected.entrySet()) {
      Collection<MetricTimeSeries> queryResult =
        metricStore.query(new MetricDataQuery(0, Integer.MAX_VALUE, Integer.MAX_VALUE,
                                              metric.getKey(), AggregationFunction.SUM,
                                              metricsContext, ImmutableList.<String>of()));
      MetricTimeSeries timeSeries = Iterables.getOnlyElement(queryResult);
      List<TimeValue> timeValues = timeSeries.getTimeValues();
      TimeValue timeValue = Iterables.getOnlyElement(timeValues);
      Assert.assertEquals(String.format("Actual value of metric: %s does not match expected", metric.getKey()),
                          metric.getValue().longValue(), timeValue.getValue());
    }
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
    prop.setProperty("num.partitions", String.valueOf(PARTITION_SIZE));
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
