package com.continuuity.internal.migrate;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.data.hbase.HBaseTestBase;
import com.continuuity.data.hbase.HBaseTestFactory;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.data2.OperationException;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.runtime.TransactionMetricsModule;
import com.continuuity.metrics.data.AggregatesScanResult;
import com.continuuity.metrics.data.AggregatesScanner;
import com.continuuity.metrics.data.AggregatesTable;
import com.continuuity.metrics.data.HbaseTableTestModule;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.continuuity.metrics.data.TimeSeriesTable;
import com.continuuity.metrics.transport.MetricsRecord;
import com.continuuity.metrics.transport.TagMetric;
import com.continuuity.test.SlowTests;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Set;

/**
 *
 */
@Category(SlowTests.class)
public class MetricsTableMigrator20to21Test {
  private static MetricsTableFactory tableFactory;
  private static MetricsTableMigrator20to21 upgrader;
  private static HBaseTestBase testHBase;

  @Test
  public void testMetricsToCopy() throws OperationException {
    AggregatesTable table = tableFactory.createAggregates(MetricsScope.REACTOR.name());
    List<TagMetric> tags = ImmutableList.of(new TagMetric("ds1", 1));
    table.update(ImmutableList.of(
      new MetricsRecord("app.f.flow.flowet", "0", "store.bytes", tags, 0L, 1),
      new MetricsRecord("app.f.flow.flowet", "0", "store.writes", tags, 0L, 1),
      new MetricsRecord("app.f.flow.flowet", "0", "store.ops", tags, 0L, 1),
      new MetricsRecord("app.f.flow.flowet", "0", "store.reads", tags, 0L, 1),
      new MetricsRecord(Constants.Metrics.DATASET_CONTEXT, "0", "dataset.store.bytes", tags, 0L, 1),
      new MetricsRecord(Constants.Metrics.DATASET_CONTEXT, "0", "dataset.store.ops", tags, 0L, 1)
    ));
    Set<String> expected = Sets.newHashSet("store.writes", "store.reads");
    Assert.assertEquals(expected, upgrader.getMetricsToCopy());
    table.clear();
  }

  @Test
  public void testUpgradeDoesNotOverflow() throws OperationException {
    AggregatesTable aggTable = tableFactory.createAggregates(MetricsScope.REACTOR.name());

    List<MetricsRecord> records = ImmutableList.of(
      new MetricsRecord("app1.f.flow1.flowlet1", "0", "store.writes",
                        tags(tag("ds1", Integer.MAX_VALUE)), 1000, Integer.MAX_VALUE),
      new MetricsRecord("app1.f.flow1.flowlet1", "0", "store.writes",
                        tags(tag("ds1", Integer.MAX_VALUE)), 1000, Integer.MAX_VALUE)
    );
    aggTable.update(records);

    // migrateIfRequired tables.  should copy all above metrics into their own "-.dataset" context.
    upgrader.migrateIfRequired();

    // check values did not overflow
    long expected = 2 * (long) Integer.MAX_VALUE;
    Assert.assertEquals(expected, getDatasetAggregate(aggTable, "dataset.store.writes", null));
    Assert.assertEquals(expected, getDatasetAggregate(aggTable, "dataset.store.writes", "ds1"));

    aggTable.clear();
  }

  @Test
  public void testUpgrade() throws OperationException {
    AggregatesTable aggTable = tableFactory.createAggregates(MetricsScope.REACTOR.name());
    TimeSeriesTable tsTable = tableFactory.createTimeSeries(MetricsScope.REACTOR.name(), 1);

    List<MetricsRecord> records = ImmutableList.of(
      // 3 seconds, writes to both ds1 and ds2 from app1.f.flow1.flowlet1
      new MetricsRecord("app1.f.flow1.flowlet1", "0", "store.writes", tags(tag("ds1", 1), tag("ds2", 1)), 1000, 2),
      new MetricsRecord("app1.f.flow1.flowlet1", "0", "store.writes", tags(tag("ds1", 1), tag("ds2", 1)), 1001, 2),
      new MetricsRecord("app1.f.flow1.flowlet1", "0", "store.writes", tags(tag("ds1", 1), tag("ds2", 1)), 1002, 2),
      // 2 seconds, write to just ds1 from app1.f.flow1.flowlet1
      new MetricsRecord("app1.f.flow1.flowlet1", "0", "store.writes", tags(tag("ds1", 1)), 1003, 1),
      new MetricsRecord("app1.f.flow1.flowlet1", "0", "store.writes", tags(tag("ds1", 1)), 1004, 1),
      // 5 total reads from ds1 from app1.f.flow1.flowlet2
      new MetricsRecord("app1.f.flow1.flowlet2", "0", "store.reads", tags(tag("ds1", 1)), 1002, 2),
      new MetricsRecord("app1.f.flow1.flowlet2", "0", "store.reads", tags(tag("ds1", 1)), 1010, 3),

      // 2 writes to ds1 from a different app
      new MetricsRecord("app2.f.flow1.flowlet1", "0", "store.writes", tags(tag("ds1", 2)), 1000, 2),
      // writes to different datasets from another app
      new MetricsRecord("app3.f.flow3.flowlet3", "0", "store.writes", tags(tag("ds3", 2), tag("ds4", 3)), 1000, 5)
    );
    aggTable.update(records);
    tsTable.save(records);

    // migrateIfRequired tables.  should copy all above metrics into their own "-.dataset" context.
    upgrader.migrateIfRequired();

    // check aggregates were correctly copied
    Assert.assertEquals(15, getDatasetAggregate(aggTable, "dataset.store.writes", null));
    Assert.assertEquals(7, getDatasetAggregate(aggTable, "dataset.store.writes", "ds1"));
    Assert.assertEquals(3, getDatasetAggregate(aggTable, "dataset.store.writes", "ds2"));
    Assert.assertEquals(2, getDatasetAggregate(aggTable, "dataset.store.writes", "ds3"));
    Assert.assertEquals(3, getDatasetAggregate(aggTable, "dataset.store.writes", "ds4"));
    Assert.assertEquals(5, getDatasetAggregate(aggTable, "dataset.store.reads", null));
    Assert.assertEquals(2, getDatasetAggregate(aggTable, "dataset.store.reads", "ds1"));
    Assert.assertEquals(0, getDatasetAggregate(aggTable, "dataset.store.reads", "ds2"));
    Assert.assertEquals(0, getDatasetAggregate(aggTable, "dataset.store.reads", "ds3"));
    Assert.assertEquals(0, getDatasetAggregate(aggTable, "dataset.store.reads", "ds4"));

    aggTable.clear();
    tsTable.clear();
  }

  long getDatasetAggregate(AggregatesTable table, String metric, String tag) {
    AggregatesScanner scanner = table.scan(Constants.Metrics.DATASET_CONTEXT, metric, "0", tag);
    long out = 0;
    while (scanner.hasNext()) {
      AggregatesScanResult result = scanner.next();
      out += result.getValue();
    }
    return out;
  }

  TagMetric tag(String tag, int val) {
    return new TagMetric(tag, val);
  }

  List<TagMetric> tags(TagMetric... tags) {
    return Lists.newArrayList(tags);
  }

  @BeforeClass
  public static void init() throws Exception {
    testHBase = new HBaseTestFactory().get();
    testHBase.startHBase();
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, testHBase.getZkConnectionString());
    cConf.unset(Constants.CFG_HDFS_USER);
    cConf.setBoolean(TxConstants.DataJanitor.CFG_TX_JANITOR_ENABLE, false);
    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, testHBase.getConfiguration()),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new ZKClientModule(),
      new DataFabricDistributedModule(),
      new LocationRuntimeModule().getDistributedModules(),
      new TransactionMetricsModule(),
      new HbaseTableTestModule()
    );

    tableFactory = injector.getInstance(MetricsTableFactory.class);
    upgrader = new MetricsTableMigrator20to21(tableFactory);
  }

  @AfterClass
  public static void finish() throws Exception {
    testHBase.stopHBase();
  }
}
