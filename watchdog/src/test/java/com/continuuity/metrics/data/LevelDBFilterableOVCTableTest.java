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
import com.continuuity.test.hbase.HBaseTestBase;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.List;

/**
 *
 */
public class LevelDBFilterableOVCTableTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();
  private static MetricsTableFactory tableFactory;
  private static final int rollTime = 60;

  @Test
  public void testFuzzyRowFilter() {

  }

  @Test
  public void testSkip() throws OperationException {
    TimeSeriesTable tsTable = tableFactory.createTimeSeries("test", 1);

    long ts = 1317470400;
    List<MetricsRecord> records = Lists.newLinkedList();
    List<TagMetric> tags = Lists.newArrayList();
    String context = "app1.f.flow1.flowlet1";
    MetricsRecord record;
    // 5 seconds of metrics, for convenience, the metric value will be 0,1,...4
    for (int i = 0; i < 5; i++) {
      records.add(new MetricsRecord(context, "0", "reads", tags, ts + i, i));
    }
    tsTable.save(records);

    // add some other metrics in other contexts
    records.add(new MetricsRecord("app1.p.procedure1", "0", "reads", tags, ts, 5));
    records.add(new MetricsRecord("app1.f.flow2.flowlet1", "0", "reads", tags, ts, 10));
    // this one should get returned
    records.add(new MetricsRecord("app1.f.flow1.flowlet2", "0", "reads", tags, ts, 15));
    tsTable.save(records);

    List<String> expectedContexts = Lists.newArrayList("app1.f.flow1.flowlet2", "app1.f.flow1.flowlet1");
    int seconds_to_query = 3;
    MetricsScanQuery query = new MetricsScanQueryBuilder()
      .setContext("app1.f.flow1")
      .setMetric("reads")
      .build(ts, ts + seconds_to_query - 1);
    MetricsScanner scanner = tsTable.scan(query);

    int num_rows = 0;
    while (scanner.hasNext()) {
      MetricsScanResult result = scanner.next();

      // check the metric
      Assert.assertEquals("reads", result.getMetric());

      // check the context
      String resultContext = result.getContext();
      Assert.assertTrue(expectedContexts.contains(resultContext));
      expectedContexts.remove(resultContext);

      // check the time values depending on the context
      if (resultContext.equals("app1.f.flow1.flowlet2")) {
        TimeValue tv = result.iterator().next();
        Assert.assertEquals(tv.getTime(), ts);
        Assert.assertEquals(tv.getValue(), 15);
      } else if (resultContext.equals("app1.f.flow1.flowlet1")) {
        int value_count = 0;
        for (TimeValue tv : result) {
          Assert.assertEquals(tv.getValue(), tv.getTime() - ts);
          value_count++;
        }
        Assert.assertEquals(seconds_to_query, value_count);
      }
      num_rows++;
    }
    Assert.assertEquals(2, num_rows);
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
