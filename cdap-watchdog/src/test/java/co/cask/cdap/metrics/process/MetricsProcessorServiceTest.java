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
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.NoopMetricsContext;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.io.DatumReaderFactory;
import co.cask.cdap.internal.io.SchemaGenerator;
import co.cask.cdap.metrics.store.MetricDatasetFactory;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.tephra.TransactionManager;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Testing the basic properties of the {@link MessagingMetricsProcessorService}
 */
public class MetricsProcessorServiceTest extends MetricsProcessorServiceTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsProcessorServiceTest.class);

  private static final String SYSTEM_METRIC_PREFIX = "system.";

  @ClassRule
  public static TemporaryFolder tmpFolder1 = new TemporaryFolder();

  @Test
  public void testMetricsProcessor() throws Exception {
    injector.getInstance(TransactionManager.class).startAndWait();
    injector.getInstance(DatasetOpExecutor.class).startAndWait();
    injector.getInstance(DatasetService.class).startAndWait();

    final MetricStore metricStore = injector.getInstance(MetricStore.class);

    Set<Integer> partitions = new HashSet<>();
    for (int i = 0; i < PARTITION_SIZE; i++) {
      partitions.add(i);
    }

    // Start KafkaMetricsProcessorService after metrics are published to Kafka

    // Intentionally set queue size to a small value, so that MessagingMetricsProcessorService
    // internally can persist metrics when more messages are to be fetched
    MessagingMetricsProcessorService messagingMetricsProcessorService =
      new MessagingMetricsProcessorService(injector.getInstance(MetricDatasetFactory.class), TOPIC_PREFIX,
                                           messagingService, injector.getInstance(SchemaGenerator.class),
                                           injector.getInstance(DatumReaderFactory.class),
                                           metricStore, 1000L, 5, partitions, new NoopMetricsContext(), 50, 0,
                                           injector.getInstance(DatasetFramework.class), cConf, true);
    messagingMetricsProcessorService.startAndWait();

    long startTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    // Publish metrics with messaging service and record expected metrics
    for (int i = 10; i < 20; i++) {
      publishMessagingMetrics(i, startTime, METRICS_CONTEXT, expected, SYSTEM_METRIC_PREFIX, MetricType.COUNTER);
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
                                           metricStore, 500L, 100, partitions, new NoopMetricsContext(), 50, 0,
                                           injector.getInstance(DatasetFramework.class), cConf, true);
    messagingMetricsProcessorService.startAndWait();

    // Publish metrics after MessagingMetricsProcessorService restarts and record expected metrics
    for (int i = 20; i < 30; i++) {
      publishMessagingMetrics(i, startTime, METRICS_CONTEXT, expected, SYSTEM_METRIC_PREFIX, MetricType.GAUGE);
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
    messagingMetricsProcessorService.stopAndWait();
    // Delete all metrics
    metricStore.deleteAll();
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
}
