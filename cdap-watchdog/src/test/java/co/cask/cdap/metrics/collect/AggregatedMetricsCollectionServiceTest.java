/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.metrics.transport.MetricValue;
import co.cask.cdap.test.SlowTests;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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

  private static final HashMap<String, String> EMPTY_TAGS = new HashMap<String, String>();
  private static final String NAMESPACE = "testnamespace";
  private static final String APP = "testapp";
  private static final String PROGRAM_TYPE = "f";
  private static final String PROGRAM = "testprogram";
  private static final String RUNID = "testrun";
  private static final String FLOWLET = "testflowlet";
  private static final String INSTANCE = "testInstance";
  private static final String METRIC = "metric";

  @Category(SlowTests.class)
  @Test
  public void testPublish() throws InterruptedException {
    final BlockingQueue<MetricValue> published = new LinkedBlockingQueue<MetricValue>();

    AggregatedMetricsCollectionService service = new AggregatedMetricsCollectionService() {
      @Override
      protected void publish(Iterator<MetricValue> metrics) {
        Iterators.addAll(published, metrics);
      }

      @Override
      protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(5, 1, TimeUnit.SECONDS);
      }
    };

    service.startAndWait();

    // non-empty tags.
    final Map baseTags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, NAMESPACE,
                                         Constants.Metrics.Tag.APP, APP,
                                         Constants.Metrics.Tag.PROGRAM_TYPE, PROGRAM_TYPE,
                                         Constants.Metrics.Tag.PROGRAM, PROGRAM,
                                         Constants.Metrics.Tag.RUN_ID, RUNID);

    try {
      // The first section tests with empty tags.
      // Publish couple metrics with empty tags, they should be aggregated.
      service.getCollector(EMPTY_TAGS).increment(METRIC, Integer.MAX_VALUE);
      service.getCollector(EMPTY_TAGS).increment(METRIC, 2);
      service.getCollector(EMPTY_TAGS).increment(METRIC, 3);
      service.getCollector(EMPTY_TAGS).increment(METRIC, 4);

      MetricValue record = published.poll(10, TimeUnit.SECONDS);
      Assert.assertNotNull(record);
      Assert.assertEquals(((long) Integer.MAX_VALUE) + 9L, record.getValue());

      // No publishing for 0 value metrics
      Assert.assertNull(published.poll(3, TimeUnit.SECONDS));

      // Publish a metric and wait for it so that we know there is around 1 second to publish more metrics to test.
      service.getCollector(EMPTY_TAGS).increment(METRIC, 1);
      Assert.assertNotNull(published.poll(3, TimeUnit.SECONDS));

      //update the metrics multiple times with gauge.
      service.getCollector(EMPTY_TAGS).gauge(METRIC, 1);
      service.getCollector(EMPTY_TAGS).gauge(METRIC, 2);
      service.getCollector(EMPTY_TAGS).gauge(METRIC, 3);

      // gauge just updates the value, so polling should return the most recent value written
      record = published.poll(3, TimeUnit.SECONDS);
      Assert.assertNotNull(record);
      Assert.assertEquals(3, record.getValue());

      // define collectors for non-empty tags
      MetricsCollector baseCollector = service.getCollector(baseTags);
      MetricsCollector flowletInstanceCollector = baseCollector.childCollector(Constants.Metrics.Tag.FLOWLET, FLOWLET)
        .childCollector(Constants.Metrics.Tag.INSTANCE_ID, INSTANCE);

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
      baseCollector.gauge(METRIC, Integer.MAX_VALUE);
      baseCollector.gauge(METRIC, 3);
      flowletInstanceCollector.gauge(METRIC, 6);
      flowletInstanceCollector.gauge(METRIC, 2);
      baseCollector.gauge(METRIC, 1);
      flowletInstanceCollector.gauge(METRIC, Integer.MAX_VALUE);

      // gauge just updates the value, so polling should return the most recent value written
      verifyGaugeMetricsValue(published.poll(10, TimeUnit.SECONDS));
      verifyGaugeMetricsValue(published.poll(10, TimeUnit.SECONDS));
    } finally {
      service.stopAndWait();
    }
  }

  private void verifyCounterMetricsValue(MetricValue metricValue) {
    Assert.assertNotNull(metricValue);
    Map<String, String> tags = metricValue.getTags();
    if (tags.size() == 5) {
      // base collector
      Assert.assertEquals(((long) Integer.MAX_VALUE) + 13L, metricValue.getValue());
    } else if (tags.size() == 7) {
      // flowlet collector
      Assert.assertEquals(15L, metricValue.getValue());
    }
  }

  private void verifyGaugeMetricsValue(MetricValue metricValue) {
    Assert.assertNotNull(metricValue);
    Map<String, String> tags = metricValue.getTags();
    if (tags.size() == 5) {
      // base collector
      Assert.assertEquals(1L, metricValue.getValue());
    } else if (tags.size() == 7) {
      // flowlet collector
      Assert.assertEquals((long) Integer.MAX_VALUE, metricValue.getValue());
    }
  }
}
