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
public class AggregatesTableTest {

  private static MetricsTableFactory tableFactory;
  private static HBaseTestBase testHBase;

  @Test
  public void testSimpleAggregates() throws OperationException {
    AggregatesTable aggregatesTable = tableFactory.createAggregates("agg");

    try {
      // Insert 10 metrics.
      for (int i = 1; i <= 10; i++) {
        MetricsRecord metric = new MetricsRecord("simple." + i, "runId", "metric",
                                                 ImmutableList.<TagMetric>of(), 0L, i);
        aggregatesTable.update(ImmutableList.of(metric));
      }

      // Insert again, so it'll get aggregated.
      for (int i = 1; i <= 10; i++) {
        MetricsRecord metric = new MetricsRecord("simple." + i, "runId", "metric",
                                                 ImmutableList.<TagMetric>of(), 0L, i);
        aggregatesTable.update(ImmutableList.of(metric));
      }

      // Scan
      AggregatesScanner scanner = aggregatesTable.scan("simple", "metric");
      try {
        long value = 0;
        while (scanner.hasNext()) {
          value += scanner.next().getValue();
        }

        Assert.assertEquals(110, value);
        Assert.assertEquals(10, scanner.getRowScanned());

      } finally {
        scanner.close();
      }
    } finally {
      aggregatesTable.clear();
    }
  }

  @Test
  public void testScanAllTags() throws OperationException {
    AggregatesTable aggregatesTable = tableFactory.createAggregates("aggScanAllTags");
    try {
      aggregatesTable.update(ImmutableList.of(
        new MetricsRecord("app1.f.flow1.flowlet1", "0", "metric", ImmutableList.of(
          new TagMetric("tag1", 1), new TagMetric("tag2", 2), new TagMetric("tag3", 3)), 0L, 6)
      ));

      Map<String, Long> tagValues = Maps.newHashMap();
      AggregatesScanner scanner = aggregatesTable.scanAllTags("app1.f.flow1.flowlet1", "metric");
      while (scanner.hasNext()) {
        AggregatesScanResult result = scanner.next();
        String tag = result.getTag();
        if (tag == null) {
          Assert.assertEquals(6, result.getValue());
        } else {
          Assert.assertFalse(tagValues.containsKey(result.getTag()));
          tagValues.put(result.getTag(), result.getValue());
        }
      }

      Assert.assertEquals(3, tagValues.size());
      Assert.assertEquals(1L, (long) tagValues.get("tag1"));
      Assert.assertEquals(2L, (long) tagValues.get("tag2"));
      Assert.assertEquals(3L, (long) tagValues.get("tag3"));
    } finally {
      aggregatesTable.clear();
    }
  }

  @Test
  public void testClear() throws OperationException {
    AggregatesTable aggregatesTable = tableFactory.createAggregates("aggDelete");

    for (int i = 1; i <= 10; i++) {
      MetricsRecord metric = new MetricsRecord("simple." + i, "runId", "metric",
                                               ImmutableList.<TagMetric>of(), 0L, i);
      aggregatesTable.update(ImmutableList.of(metric));
    }

    // Insert again, so it'll get aggregated.
    for (int i = 1; i <= 10; i++) {
      MetricsRecord metric = new MetricsRecord("simple." + i, "runId", "metric",
                                               ImmutableList.<TagMetric>of(), 0L, i);
      aggregatesTable.update(ImmutableList.of(metric));
    }

    aggregatesTable.clear();

    // Scan
    AggregatesScanner scanner = aggregatesTable.scan("simple", "metric");
    try {
      long value = 0;
      while (scanner.hasNext()) {
        value += scanner.next().getValue();
      }

      Assert.assertEquals(0, value);
      Assert.assertEquals(0, scanner.getRowScanned());

    } finally {
      scanner.close();
    }
  }


  @Test
  public void testRowFilter() throws OperationException {
    AggregatesTable aggregatesTable = tableFactory.createAggregates("agg");

    try {
      // Insert 20 different metrics from the same context.
      List<MetricsRecord> records = Lists.newArrayList();
      for (int i = 10; i < 30; i++) {
        records.add(new MetricsRecord("context." + (i % 10), "runId", "metric." + (i / 10) + ".value",
                                      ImmutableList.<TagMetric>of(), 0L, i));
      }
      aggregatesTable.update(records);

      // Scan 10 of them
      AggregatesScanner scanner = aggregatesTable.scan("context", "metric.1");
      long value = 0;
      while (scanner.hasNext()) {
        value += scanner.next().getValue();
      }

      // Only 10 - 19 are summed up.
      Assert.assertEquals(145, value);
      Assert.assertEquals(10, scanner.getRowScanned());

    } finally {
      aggregatesTable.clear();
    }
  }

  @Test
  public void testTags() throws OperationException {
    AggregatesTable aggregatesTable = tableFactory.createAggregates("agg");

    try {
      // Insert 10 metrics, each with 20 tags.
      for (int i = 0; i < 10; i++) {
        List<TagMetric> tags = Lists.newArrayList();
        for (int j = 10; j < 30; j++) {
          tags.add(new TagMetric("tag." + (j / 10) + "." + (j % 10), j));
        }

        aggregatesTable.update(ImmutableList.of(
          new MetricsRecord("context." + i, "runId", "metric", tags, 0L, i)
        ));
      }

      AggregatesScanner scanner = aggregatesTable.scan("context", "metric", "runId", "tag.1");
      long value = 0;
      while (scanner.hasNext()) {
        AggregatesScanResult result = scanner.next();
        Assert.assertTrue(result.getTag().startsWith("tag.1"));
        value += result.getValue();
      }

      Assert.assertEquals(1450, value);

    } finally {
      aggregatesTable.clear();
    }
  }

  @Test
  public void testDeleteContext() throws OperationException {
    AggregatesTable aggregatesTable = tableFactory.createAggregates("agg");

    try {
      List<TagMetric> tags = Lists.newArrayList();
      tags.add(new TagMetric("tag.1", 5));
      tags.add(new TagMetric("tag.2", 10));

      aggregatesTable.update(ImmutableList.of(
        // context, runId, metric, tags, timestamp, value
        new MetricsRecord("context.0", "0", "metric.0", tags, 0L, 100),
        new MetricsRecord("context.0", "0", "metric.1", tags, 0L, 100),
        new MetricsRecord("context.1", "0", "metric.0", tags, 0L, 100),
        new MetricsRecord("context.1", "0", "metric.1", tags, 0L, 100),
        new MetricsRecord("context.2", "0", "metric.0", tags, 0L, 100),
        new MetricsRecord("context.2", "0", "metric.1", tags, 0L, 100)
      ));

      // check values were correctly written
      long total = sumScan(aggregatesTable, "context", "metric", "0", null);
      Assert.assertEquals(600, total);

      // should delete 2 entries
      aggregatesTable.delete("context.0");
      // make sure no context.0 entries are left
      total = sumScan(aggregatesTable, "context.0", "metric", "0", null);
      Assert.assertEquals(0, total);
      // make sure the other entries were not mistakenly deleted
      total = sumScan(aggregatesTable, "context", "metric", "0", null);
      Assert.assertEquals(400, total);
      // check tagged entries for context.0 were deleted correctly too
      total = sumScan(aggregatesTable, "context.0", "metric", "0", "tag");
      Assert.assertEquals(0, total);
      // check tagged entries for other contexts were not mistakenly deleted
      total = sumScan(aggregatesTable, "context", "metric", "0", "tag");
      Assert.assertEquals(60, total);


      // should delete everything
      aggregatesTable.delete("context");
      total = sumScan(aggregatesTable, "context", "metric", "0", null);
      Assert.assertEquals(0, total);
      // check tagged entries were deleted correctly too
      total = sumScan(aggregatesTable, "context", "metric", "0", "tag");
      Assert.assertEquals(0, total);
    } finally {
      aggregatesTable.clear();
    }
  }

  @Test
  public void testDeleteContextAndMetric() throws OperationException {
    AggregatesTable aggregatesTable = tableFactory.createAggregates("agg");

    try {
      List<TagMetric> tags = Lists.newArrayList();
      tags.add(new TagMetric("tag.1", 5));
      tags.add(new TagMetric("tag.2", 10));

      aggregatesTable.update(ImmutableList.of(
        // context, runId, metric, tags, timestamp, value
        new MetricsRecord("context.0", "0", "metric.0", tags, 0L, 100),
        new MetricsRecord("context.0", "0", "metric.1", tags, 0L, 100),
        new MetricsRecord("context.1", "0", "metric.0", tags, 0L, 100),
        new MetricsRecord("context.1", "0", "metric.1", tags, 0L, 100),
        new MetricsRecord("context.2", "0", "metric.0", tags, 0L, 100),
        new MetricsRecord("context.2", "0", "metric.1", tags, 0L, 100)
      ));

      // check values were correctly written
      long total = sumScan(aggregatesTable, "context", "metric", "0", null);
      Assert.assertEquals(600, total);

      // should delete 1 entry
      aggregatesTable.delete("context.0", "metric.0");
      // make sure context.0, metric.0 was correctly deleted
      total = sumScan(aggregatesTable, "context.0", "metric.0", "0", null);
      Assert.assertEquals(0, total);
      // make sure other metric.0 entries were not mistakenly deleted
      total = sumScan(aggregatesTable, "context", "metric.0", "0", null);
      Assert.assertEquals(200, total);
      total = sumScan(aggregatesTable, "context", "metric.1", "0", null);
      Assert.assertEquals(300, total);
      // check tagged entries for context.0, metric.0 were deleted correctly too
      total = sumScan(aggregatesTable, "context.0", "metric.0", "0", "tag");
      Assert.assertEquals(0, total);
      // check tagged entries for other contexts and metrics were not mistakenly deleted
      total = sumScan(aggregatesTable, "context", "metric", "0", "tag");
      Assert.assertEquals(75, total);

      // should delete 2 entries
      aggregatesTable.delete("context.1", "metric");
      // make sure all context.1 metrics were correctly deleted
      total = sumScan(aggregatesTable, "context.1", "metric", "0", null);
      Assert.assertEquals(0, total);
      // make sure other metrics were not mistakenly deleted
      total = sumScan(aggregatesTable, "context", "metric.0", "0", null);
      Assert.assertEquals(100, total);
      total = sumScan(aggregatesTable, "context", "metric.1", "0", null);
      Assert.assertEquals(200, total);
      // check tagged entries for context.1 were deleted correctly too
      total = sumScan(aggregatesTable, "context.1", "metric", "0", "tag");
      Assert.assertEquals(0, total);
      // check tagged entries for other contexts and metrics were not mistakenly deleted
      total = sumScan(aggregatesTable, "context", "metric", "0", "tag");
      Assert.assertEquals(45, total);

      // should delete remaining entries
      aggregatesTable.delete(null, "metric");
      total = sumScan(aggregatesTable, "context.1", "metric", "0", null);
      Assert.assertEquals(0, total);
      // check tagged entries were deleted correctly too
      total = sumScan(aggregatesTable, "context", "metric", "0", "tag");
      Assert.assertEquals(0, total);
    } finally {
      aggregatesTable.clear();
    }
  }

  @Test
  public void testDeletes() throws OperationException {
    AggregatesTable aggregatesTable = tableFactory.createAggregates("agg");

    try {
      String metric = "metric";
      String runId = "runId";
      String tag1 = "tag.5";
      String tag2 = "tag.10";
      // Insert 10 metrics, each with 2 tags.
      for (int i = 0; i < 10; i++) {
        List<TagMetric> tags = Lists.newArrayList();
        tags.add(new TagMetric(tag1, 5));
        tags.add(new TagMetric(tag2, 10));

        aggregatesTable.update(ImmutableList.of(
          new MetricsRecord("context." + i, runId, metric, tags, 0L, 100)
        ));
      }

      // try deleting a row
      aggregatesTable.delete("context.0");
      long total = sumScan(aggregatesTable, "context", metric, runId, null);
      Assert.assertEquals(900, total);
      total = sumScan(aggregatesTable, "context", metric, runId, tag2);
      Assert.assertEquals(90, total);
      total = sumScan(aggregatesTable, "context", metric, runId, tag1);
      Assert.assertEquals(45, total);

      // try deleting one tag in a row
      aggregatesTable.delete("context.1", metric, runId, tag1);
      total = sumScan(aggregatesTable, "context", metric, runId, null);
      Assert.assertEquals(900, total);
      total = sumScan(aggregatesTable, "context", metric, runId, tag2);
      Assert.assertEquals(90, total);
      total = sumScan(aggregatesTable, "context", metric, runId, tag1);
      Assert.assertEquals(40, total);

      // delete another tag
      aggregatesTable.delete("context.1", metric, runId, tag2);
      total = sumScan(aggregatesTable, "context", metric, runId, null);
      Assert.assertEquals(900, total);
      total = sumScan(aggregatesTable, "context", metric, runId, tag2);
      Assert.assertEquals(80, total);
      total = sumScan(aggregatesTable, "context", metric, runId, tag1);
      Assert.assertEquals(40, total);

      aggregatesTable.delete("context", metric, runId, MetricsConstants.EMPTY_TAG, tag1, tag2);
      total = sumScan(aggregatesTable, "context", metric, runId, tag1);
      Assert.assertEquals(0, total);
      total = sumScan(aggregatesTable, "context", metric, runId, tag2);
      Assert.assertEquals(0, total);
      total = sumScan(aggregatesTable, "context", metric, runId, MetricsConstants.EMPTY_TAG);
      Assert.assertEquals(0, total);
    } finally {
      aggregatesTable.clear();
    }
  }

  @Test
  public void testSwap() throws OperationException {
    AggregatesTable aggregatesTable = tableFactory.createAggregates("aggSave");
    try {
      checkSwapForTag(aggregatesTable, null);
      checkSwapForTag(aggregatesTable, "tag");
    } finally {
      aggregatesTable.clear();
    }
  }

  private void checkSwapForTag(AggregatesTable aggregatesTable, String tag) throws OperationException {
    // table is empty, this should fail
    Assert.assertFalse(aggregatesTable.swap("context", "metric", "0", tag, 0L, 1L));
    // check we really didn't write anything
    Assert.assertFalse(aggregatesTable.scan("context", "metric", "0", tag).hasNext());

    // entry does not exist as expected, the write should go through
    Assert.assertTrue(aggregatesTable.swap("context", "metric", "0", tag, null, 1L));
    // check it actually wrote
    Assert.assertEquals(1, sumScan(aggregatesTable, "context", "metric", "0", tag));

    // entry does not match values, should fail
    Assert.assertFalse(aggregatesTable.swap("context", "metric", "0", tag, 5L, 1L));
    // check it really didn't write
    Assert.assertEquals(1, sumScan(aggregatesTable, "context", "metric", "0", tag));

    // entry does match values, should succeed
    Assert.assertTrue(aggregatesTable.swap("context", "metric", "0", tag, 1L, 5L));
    // check it really wrote
    Assert.assertEquals(5, sumScan(aggregatesTable, "context", "metric", "0", tag));
  }

  private long sumScan(AggregatesTable table, String context, String metric, String runId, String tag) {
    AggregatesScanner scanner = table.scan(context, metric, runId, tag);
    long value = 0;
    while (scanner.hasNext()) {
      AggregatesScanResult result = scanner.next();
      value += result.getValue();
    }
    return value;
  }

  @BeforeClass
  public static void init() throws Exception {
    testHBase = new HBaseTestFactory().get();
    testHBase.startHBase();
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, testHBase.getZkConnectionString());
    cConf.unset(Constants.CFG_HDFS_USER);
    cConf.setBoolean(TxConstants.DataJanitor.CFG_TX_JANITOR_ENABLE, false);
    Injector injector = Guice.createInjector(new ConfigModule(cConf, testHBase.getConfiguration()),
                                             new DiscoveryRuntimeModule().getDistributedModules(),
                                             new ZKClientModule(),
                                             new DataFabricDistributedModule(),
                                             new LocationRuntimeModule().getDistributedModules(),
                                             new TransactionMetricsModule(),
                                             new HbaseTableTestModule());

    tableFactory = injector.getInstance(MetricsTableFactory.class);
  }

  @AfterClass
  public static void finish() throws Exception {
    testHBase.stopHBase();
  }
}
