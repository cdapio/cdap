package com.continuuity.metrics.data;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.guice.ConfigModule;
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

  @BeforeClass
  public static void init() throws Exception {
    HBaseTestBase.startHBase();
    CConfiguration cConf = CConfiguration.create();
    Injector injector = Guice.createInjector(new ConfigModule(cConf, HBaseTestBase.getConfiguration()),
                                             new HbaseTableTestModule());

    tableFactory = injector.getInstance(MetricsTableFactory.class);
  }

  @AfterClass
  public static void finish() throws Exception {
    HBaseTestBase.stopHBase();
  }
}
