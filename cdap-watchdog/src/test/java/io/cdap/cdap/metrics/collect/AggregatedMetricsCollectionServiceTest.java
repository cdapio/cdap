/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
package io.cdap.cdap.metrics.collect;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.test.SlowTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Testing the basic properties of the {@link AggregatedMetricsCollectionService}.
 */
public class AggregatedMetricsCollectionServiceTest {

  private static final HashMap<String, String> EMPTY_TAGS = new HashMap<>();
  private static final String NAMESPACE = "testnamespace";
  private static final String APP = "testapp";
  private static final String SERVICE = "testservice";
  private static final String RUNID = "testrun";
  private static final String HANDLER = "testhandler";
  private static final String INSTANCE = "testInstance";
  private static final String METRIC = "metric";
  private static final String GAUGE_METRIC = "gaugeMetric";

  private Long getMetricValue(Collection<MetricValue> metrics, String metricName) {
    for (MetricValue metricValue : metrics) {
      if (metricValue.getName().equals(metricName)) {
        return metricValue.getValue();
      }
    }
    return null;
  }

  @Category(SlowTests.class)
  @Test
  public void testPublish() throws InterruptedException {
    final BlockingQueue<MetricValues> published = new LinkedBlockingQueue<>();

    AggregatedMetricsCollectionService service = new AggregatedMetricsCollectionService(1000L) {
      @Override
      protected void publish(Iterator<MetricValues> metrics) {
        Iterators.addAll(published, metrics);
      }
    };

    service.startAndWait();

    // non-empty tags.
    final Map<String, String> baseTags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, NAMESPACE,
                                                         Constants.Metrics.Tag.APP, APP,
                                                         Constants.Metrics.Tag.SERVICE, SERVICE,
                                                         Constants.Metrics.Tag.RUN_ID, RUNID);

    try {
      // The first section tests with empty tags.
      // Publish couple metrics with empty tags, they should be aggregated.
      service.getContext(EMPTY_TAGS).increment(METRIC, Integer.MAX_VALUE);
      service.getContext(EMPTY_TAGS).increment(METRIC, 2);
      service.getContext(EMPTY_TAGS).increment(METRIC, 3);
      service.getContext(EMPTY_TAGS).increment(METRIC, 4);

      verifyCounterMetricsValue(published, ImmutableMap.of(0, ImmutableMap.of(METRIC, 9L + Integer.MAX_VALUE)));

      // No publishing for 0 value metrics
      Assert.assertNull(published.poll(3, TimeUnit.SECONDS));

      //update the metrics multiple times with gauge.
      service.getContext(EMPTY_TAGS).gauge(GAUGE_METRIC, 1);
      service.getContext(EMPTY_TAGS).gauge(GAUGE_METRIC, 2);
      service.getContext(EMPTY_TAGS).gauge(GAUGE_METRIC, 3);

      // gauge just updates the value, so polling should return the most recent value written
      verifyGaugeMetricsValue(published, ImmutableMap.of(0, 3L));

      // define collectors for non-empty tags
      MetricsContext baseCollector = service.getContext(baseTags);
      MetricsContext metricsContext = baseCollector.childContext(Constants.Metrics.Tag.HANDLER, HANDLER)
        .childContext(Constants.Metrics.Tag.INSTANCE_ID, INSTANCE);

      // increment metrics for various collectors
      baseCollector.increment(METRIC, Integer.MAX_VALUE);
      metricsContext.increment(METRIC, 5);
      baseCollector.increment(METRIC, 10);
      baseCollector.increment(METRIC, 3);
      metricsContext.increment(METRIC, 2);
      metricsContext.increment(METRIC, 4);
      metricsContext.increment(METRIC, 3);
      metricsContext.increment(METRIC, 1);

      // there are two collectors, verify their metrics values
      verifyCounterMetricsValue(published, ImmutableMap.of(4, ImmutableMap.of(METRIC, 13L + Integer.MAX_VALUE),
                                                           6, ImmutableMap.of(METRIC, 15L)));

      // No publishing for 0 value metrics
      Assert.assertNull(published.poll(3, TimeUnit.SECONDS));

      // gauge metrics for various collectors
      baseCollector.gauge(GAUGE_METRIC, Integer.MAX_VALUE);
      baseCollector.gauge(GAUGE_METRIC, 3);
      metricsContext.gauge(GAUGE_METRIC, 6);
      metricsContext.gauge(GAUGE_METRIC, 2);
      baseCollector.gauge(GAUGE_METRIC, 1);
      metricsContext.gauge(GAUGE_METRIC, Integer.MAX_VALUE);

      // gauge just updates the value, so polling should return the most recent value written
      verifyGaugeMetricsValue(published, ImmutableMap.of(4, 1L, 6, (long) Integer.MAX_VALUE));

      metricsContext.gauge(GAUGE_METRIC, 0);
      verifyCounterMetricsValue(published, ImmutableMap.of(6, ImmutableMap.of(GAUGE_METRIC, 0L)));
    } finally {
      service.stopAndWait();
    }
  }

  private void verifyCounterMetricsValue(BlockingQueue<MetricValues> published,
                                         Map<Integer, Map<String, Long>> expected) throws InterruptedException {
    Map<Integer, Map<String, Long>> received = new HashMap<>();
    for (Integer key : expected.keySet()) {
      received.put(key, new HashMap<>());
    }
    long timeout = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
    while (timeout > System.currentTimeMillis() && !expected.equals(received)) {
      MetricValues metricValues = published.poll(100, TimeUnit.MILLISECONDS);
      if (metricValues == null) {
        continue;
      }
      int tags = metricValues.getTags().size();
      Map<String, Long> map = expected.get(tags);
      Collection<MetricValue> values = metricValues.getMetrics();
      Assert.assertNotNull("Unexpected number of tags while verifying counter metrics value: " + tags, map);
      for (String name : map.keySet()) {
        Long receivedValue = getMetricValue(values, name);
        if (receivedValue != null) {
          Long existing = received.get(tags).get(name);
          received.get(tags).put(name, existing == null ? receivedValue : existing + receivedValue);
        }
      }
    }
    // validate that all metrics have been received
    Assert.assertEquals(expected, received);
    // validate that no further aggregates are in the queue
    Assert.assertNull(published.poll());
  }

  private void verifyGaugeMetricsValue(BlockingQueue<MetricValues> published, Map<Integer, Long> expected)
    throws InterruptedException {

    Map<Integer, Long> seen = new HashMap<>();
    long timeout = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);

    while (timeout > System.currentTimeMillis() && !expected.equals(seen)) {

      MetricValues metricValues = published.poll(100, TimeUnit.MILLISECONDS);
      if (metricValues == null) {
        continue;
      }
      int tags = metricValues.getTags().size();
      Assert.assertNotNull("Unexpected number of tags while verifying gauge metrics value: " + tags,
                           expected.get(tags));
      Long receivedValue = getMetricValue(metricValues.getMetrics(), GAUGE_METRIC);
      if (receivedValue != null) {
        seen.put(tags, receivedValue);
      }
    }
    // validate that both gauges have been received
    Assert.assertEquals(expected, seen);
    // validate that no further aggregates are in the queue
    Assert.assertNull(published.poll());
  }
}
