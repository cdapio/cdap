/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.collect;

import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.metrics.transport.MetricsRecord;
import com.continuuity.metrics.transport.TagMetric;
import com.continuuity.test.SlowTests;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Testing the basic properties of the {@link AggregatedMetricsCollectionService}.
 */
public class AggregatedMetricsCollectionServiceTest {

  @Category(SlowTests.class)
  @Test
  public void testPublish() throws InterruptedException {
    final BlockingQueue<MetricsRecord> published = new LinkedBlockingQueue<MetricsRecord>();

    AggregatedMetricsCollectionService service = new AggregatedMetricsCollectionService() {
      @Override
      protected void publish(MetricsScope scope, Iterator<MetricsRecord> metrics) {
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
      service.getCollector(MetricsScope.REACTOR, "context", "runId").gauge("metric", 1);
      service.getCollector(MetricsScope.REACTOR, "context", "runId").gauge("metric", 2);
      service.getCollector(MetricsScope.REACTOR, "context", "runId").gauge("metric", 3);
      service.getCollector(MetricsScope.REACTOR, "context", "runId").gauge("metric", 4);

      MetricsRecord record = published.poll(10, TimeUnit.SECONDS);
      Assert.assertNotNull(record);
      Assert.assertEquals(10, record.getValue());

      // No publishing for 0 value metrics
      Assert.assertNull(published.poll(3, TimeUnit.SECONDS));

      // Publish a metric and wait for it so that we know there is around 1 second to publish more metrics to test.
      service.getCollector(MetricsScope.REACTOR, "context", "runId").gauge("metric", 1);
      Assert.assertNotNull(published.poll(3, TimeUnit.SECONDS));

      // Publish metrics with tags
      service.getCollector(MetricsScope.REACTOR, "context", "runId").gauge("metric", 3, "tag1", "tag2");
      service.getCollector(MetricsScope.REACTOR, "context", "runId").gauge("metric", 4, "tag2", "tag3");

      record = published.poll(3, TimeUnit.SECONDS);
      Assert.assertNotNull(record);
      Assert.assertEquals(7, record.getValue());

      // Verify tags are aggregated individually.
      Map<String, Integer> tagMetrics = Maps.newHashMap();
      for (TagMetric tagMetric : record.getTags()) {
        tagMetrics.put(tagMetric.getTag(), tagMetric.getValue());
      }
      Assert.assertEquals(ImmutableMap.of("tag1", 3, "tag2", 7, "tag3", 4), tagMetrics);

    } finally {
      service.stopAndWait();
    }
  }
}
