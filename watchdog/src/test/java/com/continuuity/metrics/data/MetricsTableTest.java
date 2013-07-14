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
import com.continuuity.metrics.transport.MetricsRecord;
import com.continuuity.metrics.transport.TagMetric;
import com.continuuity.test.hbase.HBaseTestBase;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
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
public class MetricsTableTest {

  private static OVCTableHandle tableHandle;
  private static EntityTable entityTable;

  @Test
  public void testAggregate() throws OperationException {
    MetricsTable metricsTable = new MetricsTable(entityTable,
                                              tableHandle.getTable(Bytes.toBytes("aggregateMetric")), 1, 300);

    // 2012-10-01T12:00:00
    final long time = 1317470400;

    // Insert metrics for flow
    for (int i = 0; i < 5; i++) {
      String context = "app.f.flow.flowlet" + i;
      String metric = "input." + i;

      // Insert 500 metrics for each flowlet with the same time series.
      insertMetrics(metricsTable, context, "runId", metric, ImmutableList.of("test"), time, 0, 500, 100);
    }

    // Insert metrics for procedure
    for (int i = 0; i < 5; i++) {
      String context = "app.p.procedure" + i;
      String metric = "input." + i;

      // Insert 500 metrics for each procedure with the same time series
      insertMetrics(metricsTable, context, "runId", metric, ImmutableList.<String>of(), time, 0, 500, 100);
    }

    // Query aggregate for flow. Expect 10 rows scanned (metric per flowlet spreads across 2 rows).
    MetricsScanQuery query = new MetricsScanQueryBuilder().setContext("app.f.flow")
                                                        .setMetric("input")
                                                        .build(time, time + 1000);
    assertAggregate(query, metricsTable.scan(query), 500, 10, new Function<Long, Integer>() {
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
    assertAggregate(query, metricsTable.scan(query), 500, 10, new Function<Long, Integer>() {
      @Override
      public Integer apply(Long ts) {
        return (int) ((ts - time) * 10);
      }
    });

    // Query aggregate for app. Expected 20 rows scanned.
    query = new MetricsScanQueryBuilder().setContext("app")
                                        .setMetric("input")
                                        .build(time, time + 1000);
    assertAggregate(query, metricsTable.scan(query), 500, 20, new Function<Long, Integer>() {
      @Override
      public Integer apply(Long ts) {
        return (int) ((ts - time) * 10);
      }
    });
  }

  private void insertMetrics(MetricsTable metricsTable,
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
      }
      metricsTable.save(records);
      records.clear();
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
