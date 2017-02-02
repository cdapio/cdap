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

import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricSearchQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.MetricValue;
import co.cask.cdap.api.metrics.MetricValues;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.api.metrics.TagValue;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.internal.io.DatumReaderFactory;
import co.cask.cdap.internal.io.SchemaGenerator;

import co.cask.cdap.metrics.store.MetricDatasetFactory;
import org.apache.tephra.TransactionManager;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Testing possible race condition of the {@link MessagingMetricsProcessorService}
 */
public class MessagingMetricsProcessorServiceTest extends MetricsProcessorServiceTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(MessagingMetricsProcessorServiceTest.class);

  private static final int PARTITION_SIZE = 2;

  @Test
  public void persistMetricsTests() throws IOException, TopicNotFoundException, InterruptedException {
    injector.getInstance(TransactionManager.class).startAndWait();
    injector.getInstance(DatasetOpExecutor.class).startAndWait();
    injector.getInstance(DatasetService.class).startAndWait();

    Set<Integer> partitions = new HashSet<>();
    for (int i = 0; i < PARTITION_SIZE; i++) {
      partitions.add(i);
    }

    MockMetricStore metricStore = new MockMetricStore();

    injector.getInstance(MetricStore.class);

    MessagingMetricsProcessorService messagingMetricsProcessorService =
      new MessagingMetricsProcessorService(injector.getInstance(MetricDatasetFactory.class), TOPIC_PREFIX,
                                           partitions, messagingService, injector.getInstance(SchemaGenerator.class),
                                           injector.getInstance(DatumReaderFactory.class),
                                           metricStore,
                                           10);
    messagingMetricsProcessorService.startAndWait();
    for (int i = 0; i < 50; i++) {
      publishMessagingMetrics(i, METRICS_CONTEXT, expected, MetricType.COUNTER);
    }
    for (int i = 50; i < 100; i++) {
      publishMessagingMetrics(i, METRICS_CONTEXT, expected, MetricType.GAUGE);
    }
    Thread.sleep(7000);
    assertMetricsResult(expected, metricStore.getAllMetrics());
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
      // no-op
    }

    @Override
    public void add(MetricValues metricValues) throws Exception {

    }

    @Override
    public void add(Collection<? extends MetricValues> metricValues) throws Exception {
      for (MetricValues metric : metricValues) {
        for (MetricValue metricValue : metric.getMetrics()) {
          if (MetricType.GAUGE.equals(metricValue.getType())) {
            metricsMap.put(EXPECTED_METRIC_PREFIX + metricValue.getName(), metricValue.getValue());
          } else {
            if (!COUNTER_METRIC_NAME.equals(metricValue.getName())) {
              continue;
            }
            Long currentValue = metricsMap.get(EXPECTED_COUNTER_METRIC_NAME);
            if (currentValue == null) {
              metricsMap.put(EXPECTED_COUNTER_METRIC_NAME, 1L);
            } else {
              metricsMap.put(EXPECTED_COUNTER_METRIC_NAME, currentValue + 1);
            }
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
