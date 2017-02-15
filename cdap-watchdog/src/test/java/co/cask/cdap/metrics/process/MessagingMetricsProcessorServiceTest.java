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

import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricSearchQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.MetricValue;
import co.cask.cdap.api.metrics.MetricValues;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.metrics.NoopMetricsContext;
import co.cask.cdap.api.metrics.TagValue;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.internal.io.DatumReaderFactory;
import co.cask.cdap.internal.io.SchemaGenerator;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import org.apache.tephra.TransactionManager;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Testing possible race condition of the {@link MessagingMetricsProcessorService}
 */
public class MessagingMetricsProcessorServiceTest extends MetricsProcessorServiceTestBase {

  private static final int PARTITION_SIZE = 2;

  @Test
  public void persistMetricsTests()
    throws Exception {

    injector.getInstance(TransactionManager.class).startAndWait();
    injector.getInstance(DatasetOpExecutor.class).startAndWait();
    injector.getInstance(DatasetService.class).startAndWait();

    Set<Integer> partitions = new HashSet<>();
    for (int i = 0; i < PARTITION_SIZE; i++) {
      partitions.add(i);
    }

    for (int iteration = 0; iteration < 50; iteration++) {
      // First publish all metrics before MessagingMetricsProcessorService starts, so that fetchers of different topics
      // will fetch metrics concurrently.
      for (int i = 0; i < 50; i++) {
        // TOPIC_PREFIX + (i % PARTITION_SIZE) decides which topic the metric is published to
        publishMessagingMetrics(i, METRICS_CONTEXT, expected, "", MetricType.COUNTER);
      }
      for (int i = 50; i < 100; i++) {
        // TOPIC_PREFIX + (i % PARTITION_SIZE) decides which topic the metric is published to
        publishMessagingMetrics(i, METRICS_CONTEXT, expected, "", MetricType.GAUGE);
      }

      final MockMetricStore metricStore = new MockMetricStore();
      // Create new MessagingMetricsProcessorService instance every time because the same instance cannot be started
      // again after it's stopped
      MessagingMetricsProcessorService messagingMetricsProcessorService =
        new MessagingMetricsProcessorService(injector.getInstance(MetricDatasetFactory.class), TOPIC_PREFIX,
                                             messagingService, injector.getInstance(SchemaGenerator.class),
                                             injector.getInstance(DatumReaderFactory.class), metricStore,
                                             5, partitions, new NoopMetricsContext(), 50);
      messagingMetricsProcessorService.startAndWait();

      // Wait for the 1 aggregated counter metric (with value 50) and 50 gauge metrics to be stored in the metricStore
      Tasks.waitFor(51, new Callable<Integer>() {
        @Override
        public Integer call() throws Exception {
          return metricStore.getAllMetrics().size();
        }
      }, 15, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);

      assertMetricsResult(expected, metricStore.getAllMetrics());
      // Clear metricStore and expected results for the next iteration
      metricStore.deleteAll();
      expected.clear();
      // Stop messagingMetricsProcessorService
      messagingMetricsProcessorService.stopAndWait();
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

    private final Map<String, Long> metricsMap = new HashMap<>();

    @Override
    public void setMetricsContext(MetricsContext metricsContext) {

    }

    @Override
    public void add(MetricValues metricValues) throws Exception {

    }

    @Override
    public void add(Collection<? extends MetricValues> metricValues) throws Exception {
      for (MetricValues metric : metricValues) {
        for (MetricValue metricValue : metric.getMetrics()) {
          // Skip metrics generated by MessagingMetricsProcessorService
          if (!COUNTER_METRIC_NAME.equals(metricValue.getName()) &&
            !metricValue.getName().startsWith(GAUGE_METRIC_NAME_PREFIX)) {
            continue;
          }
          // Increment the metric's value if it already exists, or insert the metric value
          Long currentValue = metricsMap.get(metricValue.getName());
          if (currentValue == null) {
            metricsMap.put(metricValue.getName(), metricValue.getValue());
          } else {
            metricsMap.put(metricValue.getName(), currentValue + metricValue.getValue());
          }
        }
      }
    }

    @Override
    public Collection<MetricTimeSeries> query(MetricDataQuery query) {
      return null;
    }

    @Override
    public void deleteBefore(long timestamp) throws Exception {

    }

    @Override
    public void delete(MetricDeleteQuery query) throws Exception {

    }

    @Override
    public void deleteAll() throws Exception {
      metricsMap.clear();
    }

    @Override
    public Collection<TagValue> findNextAvailableTags(MetricSearchQuery query) throws Exception {
      return null;
    }

    @Override
    public Collection<String> findMetricNames(MetricSearchQuery query) throws Exception {
      return null;
    }

    Map<String, Long> getAllMetrics() {
      return metricsMap;
    }
  }
}
