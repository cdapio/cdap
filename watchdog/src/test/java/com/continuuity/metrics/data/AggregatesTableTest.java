package com.continuuity.metrics.data;

import com.continuuity.data2.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.data.runtime.DataFabricDistributedModule;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.transport.MetricsRecord;
import com.continuuity.metrics.transport.TagMetric;
import com.continuuity.test.hbase.HBaseTestBase;
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
public class AggregatesTableTest {

  private static MetricsTableFactory tableFactory;

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
  public void testDeleteAggregates() throws OperationException {
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
    HBaseTestBase.startHBase();
    CConfiguration cConf = CConfiguration.create();
    cConf.unset(Constants.CFG_HDFS_USER);
    cConf.setBoolean(Constants.Transaction.DataJanitor.CFG_TX_JANITOR_ENABLE, false);
    Injector injector = Guice.createInjector(new ConfigModule(cConf, HBaseTestBase.getConfiguration()),
                                             new DataFabricDistributedModule(cConf, HBaseTestBase.getConfiguration()),
                                             new LocationRuntimeModule().getDistributedModules(),
                                             new HbaseTableTestModule());

    tableFactory = injector.getInstance(MetricsTableFactory.class);
  }

  @AfterClass
  public static void finish() throws Exception {
    HBaseTestBase.stopHBase();
  }
}
