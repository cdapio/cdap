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
package co.cask.cdap.metrics.collect;

import co.cask.cdap.api.metrics.MetricValue;
import co.cask.cdap.api.metrics.MetricValues;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.test.SlowTests;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Testing the basic properties of the {@link AggregatedMetricsCollectionService}.
 */
public class AggregatedMetricsCollectionServiceTest {

  private static final HashMap<String, String> EMPTY_TAGS = new HashMap<>();
  private static final String NAMESPACE = "testnamespace";
  private static final String APP = "testapp";
  private static final String FLOW = "testprogram";
  private static final String RUNID = "testrun";
  private static final String FLOWLET = "testflowlet";
  private static final String INSTANCE = "testInstance";
  private static final String METRIC = "metric";
  private static final String GAUGE_METRIC = "gaugeMetric";

  private long getMetricValue(Collection<MetricValue> metrics, String metricName) {
    Iterator<MetricValue> metricsItor = metrics.iterator();
    while (metricsItor.hasNext()) {
      MetricValue metricValue = metricsItor.next();
      if (metricValue.getName().equals(metricName)) {
        return metricValue.getValue();
      }
    }
    return 0;
  }

  @Category(SlowTests.class)
  @Test
  public void testPublish() throws InterruptedException {
    final BlockingQueue<MetricValues> published = new LinkedBlockingQueue<>();

    AggregatedMetricsCollectionService service = new AggregatedMetricsCollectionService() {
      @Override
      protected void publish(Iterator<MetricValues> metrics) {
        Iterators.addAll(published, metrics);
      }

      @Override
      protected long getInitialDelayMillis() {
        return 5000L;
      }

      @Override
      protected long getPeriodMillis() {
        return 1000L;
      }
    };

    service.startAndWait();

    // non-empty tags.
    final Map baseTags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, NAMESPACE,
                                         Constants.Metrics.Tag.APP, APP,
                                         Constants.Metrics.Tag.FLOW, FLOW,
                                         Constants.Metrics.Tag.RUN_ID, RUNID);

    try {
      // The first section tests with empty tags.
      // Publish couple metrics with empty tags, they should be aggregated.
      service.getContext(EMPTY_TAGS).increment(METRIC, Integer.MAX_VALUE);
      service.getContext(EMPTY_TAGS).increment(METRIC, 2);
      service.getContext(EMPTY_TAGS).increment(METRIC, 3);
      service.getContext(EMPTY_TAGS).increment(METRIC, 4);

      MetricValues record = published.poll(10, TimeUnit.SECONDS);
      Assert.assertNotNull(record);

      Assert.assertEquals(((long) Integer.MAX_VALUE) + 9L, getMetricValue(record.getMetrics(), METRIC));

      // No publishing for 0 value metrics
      Assert.assertNull(published.poll(3, TimeUnit.SECONDS));

      // Publish a metric and wait for it so that we know there is around 1 second to publish more metrics to test.
      service.getContext(EMPTY_TAGS).increment(METRIC, 1);
      Assert.assertNotNull(published.poll(3, TimeUnit.SECONDS));

      //update the metrics multiple times with gauge.
      service.getContext(EMPTY_TAGS).gauge(GAUGE_METRIC, 1);
      service.getContext(EMPTY_TAGS).gauge(GAUGE_METRIC, 2);
      service.getContext(EMPTY_TAGS).gauge(GAUGE_METRIC, 3);

      // gauge just updates the value, so polling should return the most recent value written
      record = published.poll(3, TimeUnit.SECONDS);
      Assert.assertNotNull(record);
      Assert.assertEquals(3, getMetricValue(record.getMetrics(), GAUGE_METRIC));

      // define collectors for non-empty tags
      MetricsContext baseCollector = service.getContext(baseTags);
      MetricsContext flowletInstanceCollector = baseCollector.childContext(Constants.Metrics.Tag.FLOWLET, FLOWLET)
        .childContext(Constants.Metrics.Tag.INSTANCE_ID, INSTANCE);

      // increment metrics for various collectors
      baseCollector.increment(METRIC, Integer.MAX_VALUE);
      flowletInstanceCollector.increment(METRIC, 5);
      baseCollector.increment(METRIC, 10);
      baseCollector.increment(METRIC, 3);
      flowletInstanceCollector.increment(METRIC, 2);
      flowletInstanceCollector.increment(METRIC, 4);
      flowletInstanceCollector.increment(METRIC, 3);
      flowletInstanceCollector.increment(METRIC, 1);

      // there are two collectors, verify their metrics values
      verifyCounterMetricsValue(published.poll(10, TimeUnit.SECONDS));
      verifyCounterMetricsValue(published.poll(10, TimeUnit.SECONDS));

      // No publishing for 0 value metrics
      Assert.assertNull(published.poll(3, TimeUnit.SECONDS));

      // gauge metrics for various collectors
      baseCollector.gauge(GAUGE_METRIC, Integer.MAX_VALUE);
      baseCollector.gauge(GAUGE_METRIC, 3);
      flowletInstanceCollector.gauge(GAUGE_METRIC, 6);
      flowletInstanceCollector.gauge(GAUGE_METRIC, 2);
      baseCollector.gauge(GAUGE_METRIC, 1);
      flowletInstanceCollector.gauge(GAUGE_METRIC, Integer.MAX_VALUE);

      // gauge just updates the value, so polling should return the most recent value written
      verifyGaugeMetricsValue(published.poll(10, TimeUnit.SECONDS));
      verifyGaugeMetricsValue(published.poll(10, TimeUnit.SECONDS));

      flowletInstanceCollector.gauge(GAUGE_METRIC, 0);
      verifyMetricsValue(published.poll(10, TimeUnit.SECONDS), 0L, GAUGE_METRIC);
    } finally {
      service.stopAndWait();
    }
  }

  @Test
  public void testServiceShutdown() throws InterruptedException, TimeoutException, ExecutionException {
    final CountDownLatch latch = new CountDownLatch(1);
    AggregatedMetricsCollectionService service = new AggregatedMetricsCollectionService() {
      @Override
      protected void publish(Iterator<MetricValues> metrics) {
        while (isRunning()) {
          try {
            latch.countDown();
            TimeUnit.MINUTES.sleep(1);
          } catch (InterruptedException e) {
            // Ignore the interrupt for testing.
            // The isRunning() should return false if the interrupt is due to service shutdown
          }
        }
      }

      @Override
      protected long getInitialDelayMillis() {
        return 0L;
      }
    };

    service.startAndWait();
    // Make sure the publish has been called
    Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));

    // Issue a stop on the service. It should interrupt the publish sleep and return quickly.
    service.stop().get(5, TimeUnit.SECONDS);
  }

  private void verifyCounterMetricsValue(MetricValues metricValues) {
    Assert.assertNotNull(metricValues);
    Map<String, String> tags = metricValues.getTags();
    if (tags.size() == 4) {
      // base collector
      Assert.assertEquals(((long) Integer.MAX_VALUE) + 13L, getMetricValue(metricValues.getMetrics(), METRIC));
    } else if (tags.size() == 6) {
      // flowlet collector
      Assert.assertEquals(15L, getMetricValue(metricValues.getMetrics(), METRIC));
    } else {
      Assert.fail("Unexpected number of tags while verifying counter metrics value - " + tags.size());
    }
  }

  private void verifyGaugeMetricsValue(MetricValues metricValues) {
    Assert.assertNotNull(metricValues);
    Map<String, String> tags = metricValues.getTags();
    if (tags.size() == 4) {
      // base collector
      Assert.assertEquals(1L, getMetricValue(metricValues.getMetrics(), GAUGE_METRIC));
    } else if (tags.size() == 6) {
      // flowlet collector
      Assert.assertEquals((long) Integer.MAX_VALUE, getMetricValue(metricValues.getMetrics(), GAUGE_METRIC));
    } else {
      Assert.fail("Unexpected number of tags while verifying gauge metrics value - " + tags.size());
    }
  }

  private void verifyMetricsValue(MetricValues metricValues, long expected, String metricName) {
    Assert.assertNotNull(metricValues);
    Assert.assertEquals(expected, getMetricValue(metricValues.getMetrics(), metricName));
  }
}
