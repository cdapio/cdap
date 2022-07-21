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

package io.cdap.cdap.metrics.process;

import io.cdap.cdap.api.metrics.MetricDataQuery;
import io.cdap.cdap.api.metrics.MetricDeleteQuery;
import io.cdap.cdap.api.metrics.MetricSearchQuery;
import io.cdap.cdap.api.metrics.MetricStore;
import io.cdap.cdap.api.metrics.MetricTimeSeries;
import io.cdap.cdap.api.metrics.MetricType;
import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.api.metrics.MetricsProcessorStatus;
import io.cdap.cdap.api.metrics.MetricsWriter;
import io.cdap.cdap.api.metrics.MetricsWriterContext;
import io.cdap.cdap.api.metrics.NoopMetricsContext;
import io.cdap.cdap.api.metrics.TagValue;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data2.datafabric.dataset.service.DatasetService;
import io.cdap.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutorService;
import io.cdap.cdap.internal.io.DatumReaderFactory;
import io.cdap.cdap.internal.io.SchemaGenerator;
import io.cdap.cdap.metrics.process.loader.MetricsWriterProvider;
import io.cdap.cdap.metrics.store.MetricDatasetFactory;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.store.StoreDefinition;
import org.apache.tephra.TransactionManager;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Testing possible race condition of the {@link MessagingMetricsProcessorManagerService}
 */
public class MessagingMetricsProcessorManagerServiceTest extends MetricsProcessorServiceTestBase {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void persistMetricsTests() throws Exception {

    injector.getInstance(TransactionManager.class).startAndWait();
    StoreDefinition.createAllTables(injector.getInstance(StructuredTableAdmin.class));
    injector.getInstance(DatasetOpExecutorService.class).startAndWait();
    injector.getInstance(DatasetService.class).startAndWait();

    Set<Integer> partitions = IntStream.range(0, cConf.getInt(Constants.Metrics.MESSAGING_TOPIC_NUM))
      .boxed().collect(Collectors.toSet());

    long startTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

    for (int iteration = 0; iteration < 50; iteration++) {
      // First publish all metrics before MessagingMetricsProcessorManagerService starts, so that fetchers of
      // different topics
      // will fetch metrics concurrently.
      for (int i = 0; i < 50; i++) {
        // TOPIC_PREFIX + (i % PARTITION_SIZE) decides which topic the metric is published to
        publishMessagingMetrics(i, startTime, METRICS_CONTEXT, expected, "", MetricType.COUNTER);
      }
      for (int i = 50; i < 100; i++) {
        // TOPIC_PREFIX + (i % PARTITION_SIZE) decides which topic the metric is published to
        publishMessagingMetrics(i, startTime, METRICS_CONTEXT, expected, "", MetricType.GAUGE);
      }
      for (int i = 0; i < 50; i++) {
        // TOPIC_PREFIX + (i % PARTITION_SIZE) decides which topic the metric is published to
        publishMessagingMetrics(i, startTime, METRICS_CONTEXT, expected, "", MetricType.DISTRIBUTION);
      }

      final MockMetricStore metricStore = new MockMetricStore();
      // Create new MessagingMetricsProcessorManagerService instance every time because the same instance cannot be
      // started
      // again after it's stopped
      MessagingMetricsProcessorManagerService messagingMetricsProcessorManagerService =
        new MessagingMetricsProcessorManagerService(cConf, injector.getInstance(MetricDatasetFactory.class),
                                                    messagingService,
                                                    injector.getInstance(SchemaGenerator.class),
                                                    injector.getInstance(DatumReaderFactory.class), metricStore,
                                                    injector.getInstance(MetricsWriterProvider.class),
                                                    partitions, new NoopMetricsContext(), 50, 0);
      messagingMetricsProcessorManagerService.startAndWait();

      // Wait for the 1 aggregated counter metric (with value 50) and 50 gauge metrics to be stored in the metricStore
      Tasks.waitFor(51, () -> metricStore.getAllCounterAndGaugeMetrics().size(), 15,
              TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      assertMetricsResult(expected, metricStore.getAllCounterAndGaugeMetrics());

      // validate metrics processor metrics
      // 50 counter, 50 gauge and 50 distributed metrics are emitted in each iteration above
      Tasks.waitFor(150L, () -> metricStore.getMetricsProcessedByMetricsProcessor(),
                    15, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      assertDistributedMetricsResult(metricStore.getAllDistributionMetrics());

      // publish a dummy metric
      // this is to force the metrics processor to publish delay metrics for all the topics
      publishMessagingMetrics(100, startTime, METRICS_CONTEXT, expected, "", MetricType.GAUGE);
      // validate the newly published metric
      Tasks.waitFor(151L, () -> metricStore.getMetricsProcessedByMetricsProcessor(),
                    15, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      // in MessagingMetricsProcessorManagerService, before persisting the metrics and topic metas, a copy of the
      // topic metas
      // containing the metrics processor delay metrics is made before making a copy of metric values.
      // Therefore, there can be a very small chance where all metric values are persisted but the corresponding
      // topic metas are not yet persisted. Wait for all topic metas to be persisted
      Tasks.waitFor(true, metricStore::isMetricsProcessorDelayEmitted, 15, TimeUnit.SECONDS);

      // Clear metricStore and expected results for the next iteration
      metricStore.deleteAll();
      expected.clear();
      // Stop messagingMetricsProcessorManagerService
      messagingMetricsProcessorManagerService.stopAndWait();
    }
  }

  @Test
  public void testWriterInititlaization() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    String filePath = tmpFolder.newFolder().getAbsolutePath();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, filePath);
    MessagingMetricsProcessorManagerService messagingMetricsProcessorManagerService =
      new MessagingMetricsProcessorManagerService(cConf, null, null, null, null, null, null, null, null, 0, 0);

    //create test metricWriter that throws exception
    MetricsWriter mockWriter = new MetricsWriter() {

      @Override
      public void close() throws IOException {

      }

      @Override
      public void write(Collection<MetricValues> metricValues) {

      }

      @Override
      public void initialize(MetricsWriterContext metricsWriterContext) {
        throw new IllegalArgumentException("Not configured correctly");
      }

      @Override
      public String getID() {
        return "test_writer_exception";
      }
    };
    try {
      messagingMetricsProcessorManagerService.initializeMetricWriter(mockWriter, null);
      Assert.fail("Expected exception, but method succeeded");
    } catch (Exception e) {
      //expected
    }

    //create the init file
    File base = new File(filePath, "metricswriters");
    base.mkdirs();
    new File(base, mockWriter.getID()).createNewFile();
    try {
      messagingMetricsProcessorManagerService.initializeMetricWriter(mockWriter, null);
    } catch (Exception e) {
      Assert.fail("Not expecting exception, but received one");
    }
  }

  private void assertDistributedMetricsResult(Map<String, MetricValue> allDistributionMetrics) {
    for (Map.Entry<String, MetricValue> entry: allDistributionMetrics.entrySet()) {
      Assert.assertNotNull(entry.getKey());
      Assert.assertEquals(BUCKET_MASK, entry.getValue().getBucketMask());
      Assert.assertArrayEquals(BUCKET_COUNTS, entry.getValue().getBucketCounts());
    }
  }

  private void assertMetricsResult(Map<String, Long> expected, Map<String, Long> actual) {
    for (Map.Entry<String, Long> metric : expected.entrySet()) {
      Long actualValue = actual.get(metric.getKey());
      Assert.assertNotNull(String.format("Cannot find the expected metric: %s in actual result", metric.getKey()),
                           actualValue);
      Assert.assertEquals(String.format("Actual value for metric: %s doesn't match expected", metric.getKey()),
                          metric.getValue().longValue(), actualValue.longValue());
    }
  }

  private static class MockMetricStore implements MetricStore {

    private final Map<String, Long> userMetricsMap = new HashMap<>();
    private final Map<String, Long> systemMetricsMap = new HashMap<>();
    private final Map<String, MetricValue> distributionMetricsMap = new HashMap<>();

    @Override
    public void setMetricsContext(MetricsContext metricsContext) {
      // no-op
    }

    @Override
    public void add(MetricValues metricValues) {
      // no-op
    }

    @Override
    public void add(Collection<? extends MetricValues> metricValues) {
      for (MetricValues metric : metricValues) {
        for (MetricValue metricValue : metric.getMetrics()) {
          // metrics generated by MessagingMetricsProcessorManagerService
          if (metricValue.getName().startsWith("metrics")) {
            addSystemMetrics(metricValue);
          }
          if (metricValue.getName().startsWith(DISTRIBUTION_METRIC_NAME_PREFIX)) {
            distributionMetricsMap.put(metricValue.getName(), metricValue);
            continue;
          }
          if (!COUNTER_METRIC_NAME.equals(metricValue.getName()) &&
            !metricValue.getName().startsWith(GAUGE_METRIC_NAME_PREFIX)) {
            continue;
          }
          // Increment the metric's value if it already exists, or insert the metric value
          userMetricsMap.merge(metricValue.getName(), metricValue.getValue(), (a, b) -> a + b);
        }
      }
    }

    private void addSystemMetrics(MetricValue metricValue) {
      if (!systemMetricsMap.containsKey(metricValue.getName())) {
        systemMetricsMap.put(metricValue.getName(), 0L);
      }
      if (metricValue.getType().equals(MetricType.GAUGE)) {
        systemMetricsMap.put(metricValue.getName(), metricValue.getValue());
      } else {
        long newValue = systemMetricsMap.get(metricValue.getName()) + metricValue.getValue();
        systemMetricsMap.put(metricValue.getName(), newValue);
      }
    }

    public long getMetricsProcessedByMetricsProcessor() {
      return systemMetricsMap.get("metrics.0.process.count");
    }

    public boolean isMetricsProcessorDelayEmitted() {
      for (int i = 0; i < cConf.getInt(Constants.Metrics.MESSAGING_TOPIC_NUM); i++) {
        if (!systemMetricsMap.containsKey(
          String.format(
            "metrics.processor.0.METRICS_STORE.topic.metrics%s.oldest.delay.ms", i)) &&
          !systemMetricsMap.containsKey(
            String.format(
              "metrics.processor.0.METRICS_STORE.topic.metrics%s.latest.delay.ms", i))) {
          return false;
        }
      }
      return true;
    }

    @Override
    public Collection<MetricTimeSeries> query(MetricDataQuery query) {
      return null;
    }

    @Override
    public void deleteBefore(long timestamp) {
      // no-op
    }

    @Override
    public void deleteTTLExpired() {
      // no-op
    }

    @Override
    public void delete(MetricDeleteQuery query) {
      // no-op
    }

    @Override
    public void deleteAll() {
      userMetricsMap.clear();
      systemMetricsMap.clear();
    }

    @Override
    public Collection<TagValue> findNextAvailableTags(MetricSearchQuery query) {
      return null;
    }

    @Override
    public Collection<String> findMetricNames(MetricSearchQuery query) {
      return null;
    }

    @Override
    public Map<String, MetricsProcessorStatus> getMetricsProcessorStats() {
      return Collections.EMPTY_MAP;
    }

    Map<String, Long> getAllCounterAndGaugeMetrics() {
      return userMetricsMap;
    }

    public Map<String, MetricValue> getAllDistributionMetrics() {
      return distributionMetricsMap;
    }
  }
}
