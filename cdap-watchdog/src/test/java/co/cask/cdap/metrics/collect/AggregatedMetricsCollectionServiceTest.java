/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.metrics.transport.MetricValue;
import co.cask.cdap.test.SlowTests;
import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Testing the basic properties of the {@link AggregatedMetricsCollectionService}.
 */
public class AggregatedMetricsCollectionServiceTest {

  private static final HashMap<String, String> EMPTY_TAGS = new HashMap<String, String>();

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
    try {
      // Publish couple metrics, they should be aggregated.
      service.getCollector(EMPTY_TAGS).increment("metric", Integer.MAX_VALUE);
      service.getCollector(EMPTY_TAGS).increment("metric", 2);
      service.getCollector(EMPTY_TAGS).increment("metric", 3);
      service.getCollector(EMPTY_TAGS).increment("metric", 4);

      MetricValue record = published.poll(10, TimeUnit.SECONDS);
      Assert.assertNotNull(record);
      Assert.assertEquals(((long) Integer.MAX_VALUE) + 9L, record.getValue());

      // No publishing for 0 value metrics
      Assert.assertNull(published.poll(3, TimeUnit.SECONDS));

      // Publish a metric and wait for it so that we know there is around 1 second to publish more metrics to test.
      service.getCollector(EMPTY_TAGS).increment("metric", 1);
      Assert.assertNotNull(published.poll(3, TimeUnit.SECONDS));

      // Publish metrics for child context
      service.getCollector(EMPTY_TAGS)
        .childCollector("tag1", "1").childCollector("tag2", "2").increment("metric", 4);

      record = published.poll(3, TimeUnit.SECONDS);
      Assert.assertNotNull(record);
      Assert.assertEquals(4, record.getValue());

      // No publishing for 0 value metrics
      Assert.assertNull(published.poll(3, TimeUnit.SECONDS));

      //update the metrics multiple times with gauge.
      service.getCollector(EMPTY_TAGS).gauge("metric", 1);
      service.getCollector(EMPTY_TAGS).gauge("metric", 2);
      service.getCollector(EMPTY_TAGS).gauge("metric", 3);

      // gauge just updates the value, so polling should return the most recent value written
      record = published.poll(3, TimeUnit.SECONDS);
      Assert.assertNotNull(record);
      Assert.assertEquals(3, record.getValue());
    } finally {
      service.stopAndWait();
    }
  }
}
