/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.transport.MetricsRecord;
import com.continuuity.metrics.transport.TagMetric;
import com.continuuity.test.hbase.HBaseTestBase;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 *
 */
public class TimeSeriesTableTest {

  private static MetricsTableFactory tableFactory;

  @Test
  public void testAggregate() throws OperationException {
    TimeSeriesTable timeSeriesTable = tableFactory.createTimeSeries("test", 1);

    // 2012-10-01T12:00:00
    final long time = 1317470400;

    // Insert metrics for flow
    for (int i = 0; i < 5; i++) {
      String context = "app.f.flow.flowlet" + i;
      String metric = "input." + i;

      // Insert 500 metrics for each flowlet with the same time series.
      insertMetrics(timeSeriesTable, context, "runId", metric, ImmutableList.of("test"), time, 0, 500, 100);
    }

    // Insert metrics for procedure
    for (int i = 0; i < 5; i++) {
      String context = "app.p.procedure" + i;
      String metric = "input." + i;

      // Insert 500 metrics for each procedure with the same time series
      insertMetrics(timeSeriesTable, context, "runId", metric, ImmutableList.<String>of(), time, 0, 500, 100);
    }

    // Query aggregate for flow. Expect 10 rows scanned (metric per flowlet spreads across 2 rows).
    MetricsScanQuery query = new MetricsScanQueryBuilder().setContext("app.f.flow")
      .setMetric("input")
      .build(time, time + 1000);
    assertAggregate(query, timeSeriesTable.scan(query), 500, 10, new Function<Long, Integer>() {
      @Override
      public Integer apply(Long ts) {
        return (int) ((ts - time) * 5);
      }
    });

    // Query aggregate for flow with tag.
    // Expected 10 rows scanned (metric per flowlet spreads across 2 rows and it shouldn't see the empty tag rows).
    query = new MetricsScanQueryBuilder().setContext("app.f.flow")
      .setMetric("input")
      .setTag("test")
      .build(time, time + 1000);
    assertAggregate(query, timeSeriesTable.scan(query), 500, 10, new Function<Long, Integer>() {
      @Override
      public Integer apply(Long ts) {
        return (int) ((ts - time) * 10);
      }
    });

    // Query aggregate for app. Expected 20 rows scanned.
    query = new MetricsScanQueryBuilder().setContext("app")
      .setMetric("input")
      .build(time, time + 1000);
    assertAggregate(query, timeSeriesTable.scan(query), 500, 20, new Function<Long, Integer>() {
      @Override
      public Integer apply(Long ts) {
        return (int) ((ts - time) * 10);
      }
    });
  }

  private void insertMetrics(TimeSeriesTable timeSeriesTable,
                             String context, String runId, String metric, Iterable<String> tags,
                             long startTime, int offset, int count, int batchSize) throws OperationException {

    List<TagMetric> tagMetrics = Lists.newLinkedList();
    List<MetricsRecord> records = Lists.newArrayListWithCapacity(batchSize);
    for (int i = offset; i < offset + count; i += batchSize) {
      for (int j = i; j < i + batchSize; j++) {
        for (String tag : tags) {
          tagMetrics.add(new TagMetric(tag, j * 2));
        }
        records.add(new MetricsRecord(context, runId, metric, tagMetrics, startTime + j, j));
        tagMetrics.clear();
      }
      timeSeriesTable.save(records);
      records.clear();
    }
  }

  @Test
  public void testClear() throws OperationException {
    TimeSeriesTable timeSeriesTable = tableFactory.createTimeSeries("testDeleteAll", 1);
    // 2012-10-01T12:00:00
    final long time = 1317470400;

    // Insert metrics for app id1 flow.
    for (int i = 0; i < 5; i++) {
      String context = "app.developer.flow.flowlet" + i;
      String metric = "input." + i;
      String specialContext = "_.";
      // Insert 500 metrics for each flowlet with the same time series.
      insertMetrics(timeSeriesTable, context, "runId", metric, ImmutableList.of("test"), time, 0, 500, 100);
      insertMetrics(timeSeriesTable, specialContext, "runId", metric, ImmutableList.of("test"), time, 0, 500, 100);

    }

    timeSeriesTable.clear();

    MetricsScanQuery query = new MetricsScanQueryBuilder().setContext("_.")
      .setMetric("input")
      .build(time, time + 1000);

    //Scan and verify 0 results for app id1
    MetricsScanner scanner = timeSeriesTable.scan(query);
    Assert.assertEquals(0, scanner.getRowScanned());

    while (scanner.hasNext()){
      MetricsScanResult result = scanner.next();
      Assert.assertTrue(false);
    }

    query = new MetricsScanQueryBuilder().setContext("app.developer.flow")
      .setMetric("input")
      .build(time, time + 1000);

    scanner = timeSeriesTable.scan(query);
    Assert.assertEquals(0, scanner.getRowScanned());

    while (scanner.hasNext()){
      MetricsScanResult result = scanner.next();
      Assert.assertTrue(false);
    }
  }

  @Test
  public void testDelete() throws OperationException {

    TimeSeriesTable timeSeriesTable = tableFactory.createTimeSeries("testDelete", 1);

    // 2012-10-01T12:00:00
    final long time = 1317470400;

    // Insert metrics for app id1 flow.
    for (int i = 0; i < 5; i++) {
      String context = "app.id1.flow.flowlet" + i;
      String metric = "input." + i;

      // Insert 500 metrics for each flowlet with the same time series.
      insertMetrics(timeSeriesTable, context, "runId", metric, ImmutableList.of("test"), time, 0, 500, 100);
    }

    //Insert metrics for app id2 flow

    for (int i = 0; i < 5; i++) {
      String context = "app.id2.flow.flowlet" + i;
      String metric = "input." + i;

      // Insert 500 metrics for each flowlet with the same time series.
      insertMetrics(timeSeriesTable, context, "runId", metric, ImmutableList.of("test"), time, 0, 500, 100);
    }

    // Query aggregate for flow. Expect 10 rows scanned (metric per flowlet spreads across 2 rows).
    MetricsScanQuery query = new MetricsScanQueryBuilder().setContext("app.id1.flow")
      .setMetric("input")
      .build(time, time + 1000);
    assertAggregate(query, timeSeriesTable.scan(query), 500, 10, new Function<Long, Integer>() {
      @Override
      public Integer apply(Long ts) {
        return (int) ((ts - time) * 5);
      }
    });

    //delete app1 metrics
    timeSeriesTable.delete("app.id1");

    //Scan and verify 0 results for app id1
    MetricsScanner scanner = timeSeriesTable.scan(query);
    Assert.assertEquals(0, scanner.getRowScanned());

    while (scanner.hasNext()){
      MetricsScanResult result = scanner.next();
      Assert.assertTrue(false);
    }

    //App id2 should still have all entries.
    query = new MetricsScanQueryBuilder().setContext("app.id2.flow")
      .setMetric("input")
      .build(time, time + 1000);
    assertAggregate(query, timeSeriesTable.scan(query), 500, 10, new Function<Long, Integer>() {
      @Override
      public Integer apply(Long ts) {
        return (int) ((ts - time) * 5);
      }
    });
  }

  /**
   * Checks the metric scan result by computing aggregate.
   * @param query
   * @param scanner
   * @param expectedCount
   * @param computeExpected Function to compute the metric value for a given timestamp.
   */
  private void assertAggregate(MetricsScanQuery query, MetricsScanner scanner,
                               int expectedCount, int expectedRowScanned, Function<Long, Integer> computeExpected) {
    List<Iterable<TimeValue>> timeValues = Lists.newArrayList();
    while (scanner.hasNext()) {
      MetricsScanResult result = scanner.next();
      Assert.assertTrue(result.getContext().startsWith(query.getContextPrefix()));
      Assert.assertTrue(result.getMetric().startsWith(query.getMetricPrefix()));
      if (query.getTagPrefix() == null) {
        Assert.assertNull(result.getTag());
      } else {
        Assert.assertTrue(result.getTag().startsWith(query.getTagPrefix()));
      }

      timeValues.add(result);
    }

    int count = 0;
    for (TimeValue tv : new TimeValueAggregator(timeValues)) {
      Assert.assertEquals(computeExpected.apply(tv.getTime()).intValue(), tv.getValue());
      count++;
    }
    Assert.assertEquals(expectedCount, count);

    Assert.assertEquals(expectedRowScanned, scanner.getRowScanned());

  }

  @BeforeClass
  public static void init() throws Exception {
    HBaseTestBase.startHBase();
    CConfiguration cConf = CConfiguration.create();
    cConf.set(MetricsConstants.ConfigKeys.TIME_SERIES_TABLE_ROLL_TIME, "300");

    Injector injector = Guice.createInjector(new ConfigModule(cConf, HBaseTestBase.getConfiguration()),
                                             new HbaseTableTestModule());

    tableFactory = injector.getInstance(MetricsTableFactory.class);
  }

  @AfterClass
  public static void finish() throws Exception {
    HBaseTestBase.stopHBase();
  }
}
