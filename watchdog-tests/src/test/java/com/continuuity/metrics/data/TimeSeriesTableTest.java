/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.hbase.HBaseTestFactory;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data2.OperationException;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.runtime.TransactionMetricsModule;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.transport.MetricsRecord;
import com.continuuity.metrics.transport.TagMetric;
import com.continuuity.test.SlowTests;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Map;

/**
 *
 */
@Category(SlowTests.class)
public class TimeSeriesTableTest {

  private static MetricsTableFactory tableFactory;
  private static HBaseTestBase testHBase;

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
      String specialContext = "_._";
      // Insert 500 metrics for each flowlet with the same time series.
      insertMetrics(timeSeriesTable, context, "runId", metric, ImmutableList.of("test"), time, 0, 500, 100);
      insertMetrics(timeSeriesTable, specialContext, "runId", metric, ImmutableList.of("test"), time, 0, 500, 100);
    }

    // verify that some metrics are there for both contexts
    MetricsScanQuery query1 = new MetricsScanQueryBuilder()
      .setContext("_._").setMetric("input").build(time,  time + 1000);
    MetricsScanQuery query2 = new MetricsScanQueryBuilder()
      .setContext("app.developer.flow").setMetric("input").build (time, time + 1000);
    Assert.assertTrue("Scan should find some metrics but hasNext is false.", timeSeriesTable.scan(query1).hasNext());
    Assert.assertTrue("Scan should find some metrics but hasNext is false.", timeSeriesTable.scan(query2).hasNext());

    timeSeriesTable.clear();

    //Scan and verify there are no results for both contexts
    Assert.assertFalse("table should be empty but scan found a next entry.", timeSeriesTable.scan(query1).hasNext());
    Assert.assertFalse("table should be empty but scan found a next entry.", timeSeriesTable.scan(query2).hasNext());
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

    while (scanner.hasNext()) {
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

  @Test
  public void testDeleteContextAndMetric() throws OperationException {

    TimeSeriesTable timeSeriesTable = tableFactory.createTimeSeries("testContextAndMetricDelete", 1);

    // 2012-10-01T12:00:00
    final long time = 1317470400;
    String runId = "runId";

    // Insert dataset metrics for 5 apps with 2 flow per app
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 2; j++) {
        String context = "app" + i + ".f.flow" + j;

        // 2 tags representing 2 datasets
        List<TagMetric> tagMetrics = Lists.newLinkedList();
        tagMetrics.add(new TagMetric("ds1", 5));
        tagMetrics.add(new TagMetric("ds2", 10));

        // 10 timepoints for each metric
        List<MetricsRecord> records = Lists.newArrayListWithCapacity(10);
        for (int k = 0; k < 10; k++) {
          records.add(new MetricsRecord(context, runId, "store.bytes", tagMetrics, time + k, 15));
          records.add(new MetricsRecord(context, runId, "store.ops", tagMetrics, time + k, 15));
          timeSeriesTable.save(records);
        }
      }
    }

    // Query aggregate
    MetricsScanQuery query = new MetricsScanQueryBuilder()
      .setContext("app1")
      .setMetric("store.ops")
      .setRunId(runId)
      .build(time, time + 10);
    // 10 datapoints from 2 rows, one for each flow
    assertAggregate(query, timeSeriesTable.scan(query), 10, 2, new Function<Long, Integer>() {
      @Override
      public Integer apply(Long ts) {
        return 30;
      }
    });

    // delete all store.ops metrics from app1.f.flow.0
    timeSeriesTable.delete("app1.f.flow0", "store.ops");
    // check everything for context app1.f.flow.0 and metric "store.ops" is deleted, and nothing gets scanned.
    query = new MetricsScanQueryBuilder()
      .setContext("app1.f.flow0")
      .setMetric("store.ops")
      .setRunId(runId)
      .build(time, time + 10);
    Assert.assertFalse("scanned for rows not deleted as expected", timeSeriesTable.scan(query).hasNext());
    // check tags got deleted too
    query = new MetricsScanQueryBuilder()
      .setContext("app1.f.flow0")
      .setMetric("store.ops")
      .setRunId(runId)
      .setTag("ds1")
      .build(time, time + 10);
    Assert.assertFalse("scanned for rows not deleted as expected", timeSeriesTable.scan(query).hasNext());

    // check other data was not mistakenly deleted
    query = new MetricsScanQueryBuilder()
      .setContext("app1.f")
      .setMetric("store.ops")
      .setRunId(runId)
      .build(time, time + 10);
    // 10 datapoints from 1 row, flow.0 should have been deleted flow
    assertAggregate(query, timeSeriesTable.scan(query), 10, 1, new Function<Long, Integer>() {
      @Override
      public Integer apply(Long ts) {
        return 15;
      }
    });

    // should delete all store metrics
    timeSeriesTable.delete(null, "store");
    query = new MetricsScanQueryBuilder()
      .setContext(null)
      .setMetric("store")
      .setRunId(runId)
      .build(time, time + 10);
    Assert.assertFalse("scanned for rows not deleted as expected", timeSeriesTable.scan(query).hasNext());
  }

  @Test
  public void testRangeDelete() throws OperationException {

    TimeSeriesTable timeSeriesTable = tableFactory.createTimeSeries("testRangeDelete", 1);

    // 2012-10-01T12:00:00
    final long time = 1317470400;
    String runId = "runId";

    // Insert dataset metrics for 5 apps with 2 flow per app
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 2; j++) {
        String context = "app" + i + ".f.flow" + j;

        // 2 tags representing 2 datasets
        List<TagMetric> tagMetrics = Lists.newLinkedList();
        tagMetrics.add(new TagMetric("ds1", 5));
        tagMetrics.add(new TagMetric("ds2", 10));

        // 10 timepoints for each metric
        List<MetricsRecord> records = Lists.newArrayListWithCapacity(10);
        for (int k = 0; k < 10; k++) {
          records.add(new MetricsRecord(context, runId, "store.bytes", tagMetrics, time + k, 15));
          records.add(new MetricsRecord(context, runId, "store.ops", tagMetrics, time + k, 15));
          timeSeriesTable.save(records);
        }
      }
    }

    // Query aggregate
    MetricsScanQuery query = new MetricsScanQueryBuilder()
      .setContext("app1.f")
      .setMetric("store.ops")
      .setRunId(runId)
      .build(time, time + 10);
    // 10 datapoints from 2 rows, one for each flow
    assertAggregate(query, timeSeriesTable.scan(query), 10, 2, new Function<Long, Integer>() {
      @Override
      public Integer apply(Long ts) {
        return 30;
      }
    });

    // delete all metrics with tag ds1
    query = new MetricsScanQueryBuilder()
      .setContext(null)
      .setMetric(null)
      .allowEmptyMetric()
      .setTag("ds1")
      .build(time, time + 10);
    timeSeriesTable.delete(query);

    // check everything is deleted, and nothing gets scanned.
    assertAggregate(query, timeSeriesTable.scan(query), 0, 0, null);

    // delete all store.bytes metrics with tag ds2.  All that should be left are metrics for store.ops with tag ds2
    // and any metrics with the empty tag.
    query = new MetricsScanQueryBuilder()
      .setContext(null)
      .setMetric("store.bytes")
      .setTag("ds2")
      .build(time, time + 10);
    timeSeriesTable.delete(query);

    // check store.bytes with tag ds2 was deleted
    assertAggregate(query, timeSeriesTable.scan(query), 0, 0, null);


    // delete half the timestamps for store.ops metrics with tag ds2.  All that should be left are half of the metrics
    // for store.ops with tag ds2 and all metrics with the empty tag.
    query = new MetricsScanQueryBuilder()
      .setContext(null)
      .setMetric("store.ops")
      .setTag("ds2")
      .build(time, time + 4);
    timeSeriesTable.delete(query);

    // check store.bytes with tag ds2 was deleted
    assertAggregate(query, timeSeriesTable.scan(query), 0, 0, null);

    // check we still have half the metrics for store.ops with tag ds2
    query = new MetricsScanQueryBuilder()
      .setContext(null)
      .setMetric("store.ops")
      .setTag("ds2")
      .build(time, time + 10);
    // expect 5 datapoints from 10 rows, one row for each app/flow pair
    assertAggregate(query, timeSeriesTable.scan(query), 5, 10, new Function<Long, Integer>() {
      @Override
      public Integer apply(Long ts) {
        // 10 store.ops * 2 flows / app * 5 apps = 50
        return 100;
      }
    });

    // check we still have both metrics with empty tag
    query = new MetricsScanQueryBuilder()
      .setContext(null)
      .setMetric("store")
      .build(time, time + 10);
    // expect 10 datapoints from 20 rows, one row for each app/flow/metric triplet
    assertAggregate(query, timeSeriesTable.scan(query), 10, 20, new Function<Long, Integer>() {
      @Override
      public Integer apply(Long ts) {
        // (15 store.bytes + 15 store.ops) * 2 flows / app * 5 apps = 300
        return 300;
      }
    });
  }

  @Test
  public void testScanAllTags() throws OperationException {

    TimeSeriesTable timeSeriesTable = tableFactory.createTimeSeries("testScanAllTags", 1);

    try {
      timeSeriesTable.save(ImmutableList.of(
        new MetricsRecord("app.f.flow.flowlet", "0", "store.bytes", ImmutableList.of(
          new TagMetric("tag1", 1), new TagMetric("tag2", 2), new TagMetric("tag3", 3)), 1234567890, 6)
      ));

      Map<String, Integer> tagValues = Maps.newHashMap();
      MetricsScanQuery query = new MetricsScanQueryBuilder()
        .setContext("app.f.flow.flowlet")
        .setMetric("store.bytes")
        .setRunId("0")
        .build(1234567890, 1234567891);
      MetricsScanner scanner = timeSeriesTable.scanAllTags(query);
      while (scanner.hasNext()) {
        MetricsScanResult result = scanner.next();
        String tag = result.getTag();
        if (tag == null) {
          Assert.assertEquals(6, result.iterator().next().getValue());
        } else {
          Assert.assertFalse(tagValues.containsKey(result.getTag()));
          tagValues.put(result.getTag(), result.iterator().next().getValue());
        }
      }

      Assert.assertEquals(3, tagValues.size());
      Assert.assertEquals(1, (int) tagValues.get("tag1"));
      Assert.assertEquals(2, (int) tagValues.get("tag2"));
      Assert.assertEquals(3, (int) tagValues.get("tag3"));
    } finally {
      timeSeriesTable.clear();
    }
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
      if (query.getContextPrefix() != null) {
        Assert.assertTrue(result.getContext().startsWith(query.getContextPrefix()));
      }
      if (query.getMetricPrefix() != null) {
        Assert.assertTrue(result.getMetric().startsWith(query.getMetricPrefix()));
      }
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
    testHBase = new HBaseTestFactory().get();
    testHBase.startHBase();
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, testHBase.getZkConnectionString());
    cConf.set(MetricsConstants.ConfigKeys.TIME_SERIES_TABLE_ROLL_TIME, "300");
    cConf.unset(Constants.CFG_HDFS_USER);
    cConf.setBoolean(TxConstants.DataJanitor.CFG_TX_JANITOR_ENABLE, false);

    Injector injector = Guice.createInjector(new ConfigModule(cConf, testHBase.getConfiguration()),
                                             new DiscoveryRuntimeModule().getDistributedModules(),
                                             new ZKClientModule(),
                                             new LocationRuntimeModule().getDistributedModules(),
                                             new DataFabricDistributedModule(),
                                             new TransactionMetricsModule(),
                                             new HbaseTableTestModule());

    tableFactory = injector.getInstance(MetricsTableFactory.class);
  }

  @AfterClass
  public static void finish() throws Exception {
    testHBase.stopHBase();
  }
}
