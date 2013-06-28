/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.data.operation.executor.ReadPointer;
import com.continuuity.data.operation.executor.Transaction;
import com.continuuity.data.operation.executor.omid.OmidTransactionException;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.TransactionResult;
import com.continuuity.data.operation.executor.omid.Undo;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.metrics.transport.MetricRecord;
import com.continuuity.metrics.transport.TagMetric;
import com.continuuity.test.hbase.HBaseTestBase;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 *
 */
public class MetricTableTest {

  private static OVCTableHandle tableHandle;
  private static EntityTable entityTable;

  @Test
  public void testSingleRow() throws OperationException {
    MetricTable metricTable = new MetricTable(entityTable,
                                              tableHandle.getTable(Bytes.toBytes("simpleMetric")), 1, 3600);

    // 2012-10-01T12:00:00
    long time = 1317470400;
    String context = "app1.f.flow1.flowlet1";
    String metric = "input";

    // Insert 400 metrics in 4 batches, starting at timestamp offset 100.
    insertMetrics(metricTable, context, metric, ImmutableList.<String>of(), time, 100, 400, 100);

    // Query the first 99 seconds, it should get empty result
    Assert.assertTrue(Iterables.isEmpty(metricTable.query(time, time + 99, context, metric)));

    // Query with data in the range, should get data
    assertMetrics(metricTable.query(time, time + 500, context, metric), time, 400);

    // No data for time after 500 seconds.
    Assert.assertTrue(Iterables.isEmpty(metricTable.query(time + 500, time + 1000, context, metric)));
  }

  @Test
  public void testMultiRow() throws OperationException {
    // Metric table with 5 mins row rollup interval
    MetricTable metricTable = new MetricTable(entityTable,
                                              tableHandle.getTable(Bytes.toBytes("multiRowMetric")), 1, 300);

    // 2012-10-01T12:00:00
    long time = 1317470400;
    String context = "app1.f.flow1.flowlet1";
    String metric = "input";

    // Insert 600 metrics in 6 batches, starting at timestamp offset 100, hence spans three rows.
    insertMetrics(metricTable, context, metric, ImmutableList.<String>of(), time, 100, 600, 100);

    // Scan the first two rows, should get 500 data points, since it was offset 100 when inserting metrics.
    assertMetrics(metricTable.query(time, time + 599, context, metric), time, 500);

    // Should get all metrics.
    assertMetrics(metricTable.query(time, Integer.MAX_VALUE, context, metric), time, 600);
  }

  @Test
  public void testAggregate() throws OperationException {
    MetricTable metricTable = new MetricTable(entityTable,
                                              tableHandle.getTable(Bytes.toBytes("aggregateMetric")), 1, 300);

    // 2012-10-01T12:00:00
    final long time = 1317470400;

    // Insert metrics for flow
    for (int i = 0; i < 5; i++) {
      String context = "app.f.flow.flowlet" + i;
      String metric = "input." + i;

      // Insert 500 metrics for each flowlet with the same time series.
      insertMetrics(metricTable, context, metric, ImmutableList.of("test"), time, 0, 500, 100);
    }

    // Insert metrics for procedure
    for (int i = 0; i < 5; i++) {
      String context = "app.p.procedure" + i;
      String metric = "input." + i;

      // Insert 500 metrics for each procedure with the same time series
      insertMetrics(metricTable, context, metric, ImmutableList.<String>of(), time, 0, 500, 100);
    }

    // Query aggregate for flow. Expect 10 rows scanned (metric per flowlet spreads across 2 rows).
    MetricScanQuery query = new MetricScanQueryBuilder().setContext("app.f.flow")
                                                        .setMetric("input")
                                                        .build(time, time + 1000);
    assertAggregate(query, metricTable.scan(query), 500, 10, new Function<Long, Integer>() {
      @Override
      public Integer apply(Long ts) {
        return (int) ((ts - time) * 5);
      }
    });

    // Query aggregate for flow with tag.
    // Expected 10 rows scanned (metric per flowlet spreads across 2 rows and it shouldn't see the empty tag rows).
    query = new MetricScanQueryBuilder().setContext("app.f.flow")
                                        .setMetric("input")
                                        .setTag("test")
                                        .build(time, time + 1000);
    assertAggregate(query, metricTable.scan(query), 500, 10, new Function<Long, Integer>() {
      @Override
      public Integer apply(Long ts) {
        return (int) ((ts - time) * 10);
      }
    });

    // Query aggregate for app. Expected 20 rows scanned.
    query = new MetricScanQueryBuilder().setContext("app")
                                        .setMetric("input")
                                        .build(time, time + 1000);
    assertAggregate(query, metricTable.scan(query), 500, 20, new Function<Long, Integer>() {
      @Override
      public Integer apply(Long ts) {
        return (int) ((ts - time) * 10);
      }
    });
  }

  private void insertMetrics(MetricTable metricTable, String context, String metric, Iterable<String> tags,
                             long startTime, int offset, int count, int batchSize) throws OperationException {

    List<TagMetric> tagMetrics = Lists.newLinkedList();
    List<MetricRecord> records = Lists.newArrayListWithCapacity(batchSize);
    for (int i = offset; i < offset + count; i += batchSize) {
      for (int j = i; j < i + batchSize; j++) {
        for (String tag : tags) {
          tagMetrics.add(new TagMetric(tag, j * 2));
        }
        records.add(new MetricRecord(context, metric, tagMetrics, startTime + j, j));
      }
      metricTable.save(records);
      records.clear();
    }
  }

  private void assertMetrics(Iterable<TimeValue> timeValues, long time, int expectedCount) {
    int count = 0;
    for (TimeValue tv : timeValues) {
      Assert.assertEquals(tv.getTimestamp() - time, tv.getValue());
      count++;
    }
    Assert.assertEquals(expectedCount, count);
  }

  /**
   * Checks the metric scan result by computing aggregate.
   * @param query
   * @param scanner
   * @param expectedCount
   * @param computeExpected Function to compute the metric value for a given timestamp.
   */
  private void assertAggregate(MetricScanQuery query, MetricScanner scanner,
                               int expectedCount, int expectedRowScanned, Function<Long, Integer> computeExpected) {
    List<Iterable<TimeValue>> timeValues = Lists.newArrayList();
    while (scanner.hasNext()) {
      MetricScanResult result = scanner.next();
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
      Assert.assertEquals(computeExpected.apply(tv.getTimestamp()).intValue(), tv.getValue());
      count++;
    }
    Assert.assertEquals(expectedCount, count);

    Assert.assertEquals(expectedRowScanned, scanner.getRowScanned());

  }

  @BeforeClass
  public static void init() throws Exception {
    HBaseTestBase.startHBase();
    Injector injector = Guice.createInjector(new ConfigModule(CConfiguration.create(),
                                                              HBaseTestBase.getConfiguration()),
                                             new MetricModule());

    tableHandle = injector.getInstance(OVCTableHandle.class);
    entityTable = new EntityTable(tableHandle.getTable(Bytes.toBytes("metricEntity")));
  }

  @AfterClass
  public static void finish() throws Exception {
    HBaseTestBase.stopHBase();
  }

  private static final class MetricModule extends AbstractModule {

    @Override
    protected void configure() {
      bind(OVCTableHandle.class).to(HBaseFilterableOVCTableHandle.class);
      bind(TransactionOracle.class).to(NoopTransactionOracle.class).in(Scopes.SINGLETON);
    }
  }

  private static final class NoopTransactionOracle implements TransactionOracle {

    @Override
    public Transaction startTransaction(boolean trackChanges) {
      throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void validateTransaction(Transaction tx) throws OmidTransactionException {
      throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void addToTransaction(Transaction tx, List<Undo> undos) throws OmidTransactionException {
      throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public TransactionResult commitTransaction(Transaction tx) throws OmidTransactionException {
      throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public TransactionResult abortTransaction(Transaction tx) throws OmidTransactionException {
      throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void removeTransaction(Transaction tx) throws OmidTransactionException {
      throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public ReadPointer getReadPointer() {
      throw new UnsupportedOperationException("Not supported");
    }
  }
}
