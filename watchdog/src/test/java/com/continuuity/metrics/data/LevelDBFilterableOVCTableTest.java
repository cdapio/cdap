/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.guice.MetricsAnnotation;
import com.continuuity.metrics.transport.MetricsRecord;
import com.continuuity.metrics.transport.TagMetric;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class LevelDBFilterableOVCTableTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();
  private static MetricsTableFactory tableFactory;
  private static final int rollTime = 60;

  @Test
  public void testAggregatesQuery() throws OperationException {
    AggregatesTable table = tableFactory.createAggregates("test");
    List<MetricsRecord> records = Lists.newLinkedList();
    List<TagMetric> tags = Lists.newArrayList();
    long ts = 1317470400;
    records.add(new MetricsRecord("app1.f.flow1.flowlet1", "0", "count.reads", tags, ts, 10));
    records.add(new MetricsRecord("app1.f.flow1.flowlet1", "0", "count.writes", tags, ts, 10));
    records.add(new MetricsRecord("app1.f.flow1.flowlet1", "0", "count.attempts", tags, ts, 10));
    records.add(new MetricsRecord("app1.f.flow1.flowlet1", "0", "count.errors", tags, ts, 10));
    records.add(new MetricsRecord("app1.f.flow1.flowlet2", "0", "count.reads", tags, ts, 20));
    records.add(new MetricsRecord("app1.f.flow1.flowlet2", "0", "count.writes", tags, ts, 20));
    records.add(new MetricsRecord("app1.p.procedure1", "0", "count", tags, ts, 50));
    table.update(records);

    // check that we get the right flow metrics for the app
    int flowlet1_count = 0;
    int flowlet2_count = 0;
    AggregatesScanner scanner = table.scan("app1.f", "count");
    while (scanner.hasNext()) {
      AggregatesScanResult result = scanner.next();
      Assert.assertTrue(result.getContext().startsWith("app1.f"));
      if (result.getContext().equals("app1.f.flow1.flowlet1")) {
        Assert.assertEquals(10, result.getValue());
        flowlet1_count++;
      } else if (result.getContext().equals("app1.f.flow1.flowlet2")) {
        Assert.assertEquals(20, result.getValue());
        flowlet2_count++;
      }
    }
    Assert.assertEquals(4, flowlet1_count);
    Assert.assertEquals(2, flowlet2_count);

    // should only get 1 row back for this metric
    scanner = table.scan("app1.f.flow1", "count.errors");
    int rows = 0;
    while (scanner.hasNext()) {
      AggregatesScanResult result = scanner.next();
      rows++;
      Assert.assertEquals("count.errors", result.getMetric());
      Assert.assertEquals("app1.f.flow1.flowlet1", result.getContext());
    }
    Assert.assertEquals(1, rows);
  }

  @Test
  public void testTimeseriesQuery() throws OperationException {
    TimeSeriesTable tsTable = tableFactory.createTimeSeries("test", 1);

    // one below the 1317470400 timebase
    long ts = 1317470399;

    List<MetricsRecord> records = Lists.newLinkedList();
    List<TagMetric> tags = Lists.newArrayList();
    Map<String, List<TimeValue>> expectedResults = Maps.newHashMap();

    int seconds_to_query = 3;

    String context = "app1.f.flow1.flowlet1";
    MetricsRecord record;
    // insert time values that the query should not return
    for (int i = -2; i < seconds_to_query + 2; i++) {
      records.add(new MetricsRecord(context, "0", "reads", tags, ts + i, i));
    }
    List<TimeValue > expectedFlowlet1Timevalues = Lists.newArrayListWithExpectedSize(seconds_to_query);
    for (int i = 0; i < seconds_to_query; i++) {
      expectedFlowlet1Timevalues.add(new TimeValue(ts + i, i));
    }
    expectedResults.put("app1.f.flow1.flowlet1", expectedFlowlet1Timevalues);

    // add some other metrics in other contexts that should not get returned
    records.add(new MetricsRecord("app1.p.procedure1", "0", "reads", tags, ts, 5));
    records.add(new MetricsRecord("app1.f.flow2.flowlet1", "0", "reads", tags, ts, 10));

    // this one should get returned
    records.add(new MetricsRecord("app1.f.flow1.flowlet2", "0", "reads", tags, ts, 15));
    records.add(new MetricsRecord("app1.f.flow1.flowlet2", "0", "reads", tags, ts + seconds_to_query - 1, 20));
    records.add(new MetricsRecord("app1.f.flow1.flowlet2", "0", "reads", tags, ts + seconds_to_query + 5, 100));
    expectedResults.put("app1.f.flow1.flowlet2",
                        Lists.newArrayList(new TimeValue(ts, 15), new TimeValue(ts + seconds_to_query - 1, 20)));
    tsTable.save(records);

    MetricsScanQuery query = new MetricsScanQueryBuilder()
      .setContext("app1.f.flow1")
      .setMetric("reads")
      .build(ts, ts + seconds_to_query - 1);
    MetricsScanner scanner = tsTable.scan(query);

    Map<String, List<TimeValue >> actualResults = Maps.newHashMap();
    while (scanner.hasNext()) {
      MetricsScanResult result = scanner.next();

      // check the metric
      Assert.assertTrue(result.getMetric().startsWith("reads"));

      List<TimeValue > metricTimeValues = actualResults.get(result.getContext());
      if (metricTimeValues == null) {
        metricTimeValues = Lists.newArrayList();
        actualResults.put(result.getContext(), metricTimeValues);
      }
      for (TimeValue tv : result) {
        metricTimeValues.add(tv);
      }
    }
    assertEqualResults(expectedResults, actualResults);
  }

  private void assertEqualResults(Map<String, List<TimeValue>> expected, Map<String, List<TimeValue>> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    for (String context : expected.keySet()) {
      List<TimeValue > expectedValues = expected.get(context);
      List<TimeValue > actualValues = actual.get(context);
      Assert.assertEquals(expectedValues.size(), actualValues.size());

      // check all the values seen are expected
      for (TimeValue actualVal : actualValues) {
        TimeValue toRemove = null;
        for (TimeValue expectedVal : expectedValues) {
          if ((expectedVal.getTime() == actualVal.getTime()) && (expectedVal.getValue() == actualVal.getValue())) {
            toRemove = expectedVal;
            break;
          }
        }
        Assert.assertNotNull(toRemove);
        expectedValues.remove(toRemove);
      }
    }
  }


  @BeforeClass
  public static void init() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(MetricsConstants.ConfigKeys.TIME_SERIES_TABLE_ROLL_TIME, String.valueOf(rollTime));

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new PrivateModule() {

        @Override
        protected void configure() {
          try {
            bind(TransactionOracle.class).to(NoopTransactionOracle.class);

            bindConstant()
              .annotatedWith(Names.named("LevelDBOVCTableHandleBasePath"))
              .to(tmpFolder.newFolder().getAbsolutePath());
            bindConstant()
              .annotatedWith(Names.named("LevelDBOVCTableHandleBlockSize"))
              .to(Constants.DEFAULT_DATA_LEVELDB_BLOCKSIZE);
            bindConstant()
              .annotatedWith(Names.named("LevelDBOVCTableHandleCacheSize"))
              .to(Constants.DEFAULT_DATA_LEVELDB_CACHESIZE);

            bind(OVCTableHandle.class)
              .annotatedWith(MetricsAnnotation.class)
              .toInstance(LevelDBFilterableOVCTableHandle.getInstance());

            bind(MetricsTableFactory.class).to(DefaultMetricsTableFactory.class).in(Scopes.SINGLETON);
            expose(MetricsTableFactory.class);

          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      });
    tableFactory = injector.getInstance(MetricsTableFactory.class);
  }
}
