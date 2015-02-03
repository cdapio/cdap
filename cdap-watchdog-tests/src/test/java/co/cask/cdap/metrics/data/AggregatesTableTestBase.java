/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.metrics.data;

import co.cask.cdap.metrics.MetricsConstants;
import co.cask.cdap.metrics.transport.MetricType;
import co.cask.cdap.metrics.transport.MetricsRecord;
import co.cask.cdap.metrics.transport.TagMetric;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * Test base for AggregatesTable tests for different mode.
 */
public abstract class AggregatesTableTestBase {

  protected abstract MetricsTableFactory getTableFactory();

  @Test
  public void testSimpleAggregates() throws Exception {
    AggregatesTable aggregatesTable = getTableFactory().createAggregates();

    try {
      // Insert 10 metrics.
      for (int i = 1; i <= 10; i++) {
        MetricsRecord metric = new MetricsRecord("simple." + i, "runId", "metric",
                                                 ImmutableList.<TagMetric>of(), 0L, i, MetricType.COUNTER);
        aggregatesTable.update(ImmutableList.of(metric));
      }

      // Insert again, so it'll get aggregated.
      for (int i = 1; i <= 10; i++) {
        MetricsRecord metric = new MetricsRecord("simple." + i, "runId", "metric",
                                                 ImmutableList.<TagMetric>of(), 0L, i, MetricType.COUNTER);
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
  public void testIntOverflow() throws Exception {
    AggregatesTable aggregatesTable = getTableFactory().createAggregates();

    // 2012-10-01T12:00:00
    final long time = 1317470400;

    // checking that we can store more than just int
    long value = Integer.MAX_VALUE * 2L;
    aggregatesTable.update(ImmutableList.of(new MetricsRecord("context", "runId", "bigmetric",
                                                              ImmutableList.<TagMetric>of(new TagMetric("tag", value)),
                                                              time, value, MetricType.COUNTER)));
    aggregatesTable.update(ImmutableList.of(new MetricsRecord("context", "runId", "bigmetric",
                                                              ImmutableList.<TagMetric>of(new TagMetric("tag", value)),
                                                              time, value, MetricType.COUNTER)));

    AggregatesScanner scanner = aggregatesTable.scan("context", "bigmetric");
    Assert.assertTrue(scanner.hasNext());
    Assert.assertEquals(value * 2, scanner.next().getValue());
  }

  @Test
  public void testGaugeValues() throws Exception {
    AggregatesTable aggregatesTable = getTableFactory().createAggregates();

    // 2012-10-01T12:00:00
    final long time = 1317470400;

    // checking that we can store more than just int
    long value = Integer.MAX_VALUE * 2L;
    aggregatesTable.update(ImmutableList.of(new MetricsRecord("context", "runId", "bigmetric",
                                                              ImmutableList.<TagMetric>of(new TagMetric("tag", value)),
                                                              time, value, MetricType.GAUGE)));


    aggregatesTable.update(ImmutableList.of(new MetricsRecord("context", "runId", "bigmetric",
                                                              ImmutableList.<TagMetric>of(new TagMetric("tag", value)),
                                                              time, value, MetricType.COUNTER)));
    aggregatesTable.update(ImmutableList.of(new MetricsRecord("context", "runId", "bigmetric",
                                                              ImmutableList.<TagMetric>of(
                                                                new TagMetric("tag", value + 10)),
                                                              time, value + 10, MetricType.GAUGE)));

    AggregatesScanner scanner = aggregatesTable.scan("context", "bigmetric");
    Assert.assertTrue(scanner.hasNext());
    Assert.assertEquals(value + 10, scanner.next().getValue());
  }

  @Test
  public void testScanAllTags() throws Exception {
    AggregatesTable aggregatesTable = getTableFactory().createAggregates();
    try {
      aggregatesTable.update(ImmutableList.of(
        new MetricsRecord("app1.f.flow1.flowlet1", "0", "metric", ImmutableList.of(
          new TagMetric("tag1", 1), new TagMetric("tag2", 2), new TagMetric("tag3", 3)), 0L, 6, MetricType.COUNTER)
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
  public void testScanTagPrefix() throws Exception {
    AggregatesTable aggregatesTable = getTableFactory().createAggregates();
    try {
      aggregatesTable.update(ImmutableList.of(
        new MetricsRecord("app1.f.flow1.flowlet1", "0", "metric", ImmutableList.of(
          new TagMetric("tag.1", 1), new TagMetric("tag.2", 2), new TagMetric("tag3", 3)), 0L, 6,
                          MetricType.COUNTER)
      ));

      // Scan with "tag" as prefix. It should includes "tag.1" and "tag.2", but not "tag3"
      Map<String, Long> tagValues = Maps.newHashMap();
      AggregatesScanner scanner = aggregatesTable.scan("app1.f.flow1.flowlet1", "metric", "0", "tag");
      while (scanner.hasNext()) {
        AggregatesScanResult result = scanner.next();
        String tag = result.getTag();
        Assert.assertTrue(tag.startsWith("tag."));
        Assert.assertFalse(tagValues.containsKey(tag));
        tagValues.put(tag, result.getValue());
      }
      scanner.close();

      Assert.assertEquals(2, tagValues.size());
      Assert.assertEquals(1L, (long) tagValues.get("tag.1"));
      Assert.assertEquals(2L, (long) tagValues.get("tag.2"));

      // Scan for "tag3" only
      scanner = aggregatesTable.scan("app1.f.flow1.flowlet1", "metric", "0", "tag3");
      while (scanner.hasNext()) {
        AggregatesScanResult result = scanner.next();
        String tag = result.getTag();
        Assert.assertEquals("tag3", tag);
        Assert.assertEquals(3L, result.getValue());
      }
      scanner.close();
    } finally {
      aggregatesTable.clear();
    }
  }

  @Test
  public void testClear() throws Exception {
    AggregatesTable aggregatesTable = getTableFactory().createAggregates();

    for (int i = 1; i <= 10; i++) {
      MetricsRecord metric = new MetricsRecord("simple." + i, "runId", "metric",
                                               ImmutableList.<TagMetric>of(), 0L, i, MetricType.COUNTER);
      aggregatesTable.update(ImmutableList.of(metric));
    }

    // Insert again, so it'll get aggregated.
    for (int i = 1; i <= 10; i++) {
      MetricsRecord metric = new MetricsRecord("simple." + i, "runId", "metric",
                                               ImmutableList.<TagMetric>of(), 0L, i, MetricType.COUNTER);
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
  public void testRowFilter() throws Exception {
    AggregatesTable aggregatesTable = getTableFactory().createAggregates();

    try {
      // Insert 20 different metrics from the same context.
      List<MetricsRecord> records = Lists.newArrayList();
      for (int i = 10; i < 30; i++) {
        records.add(new MetricsRecord("context." + (i % 10), "runId", "metric." + (i / 10) + ".value",
                                      ImmutableList.<TagMetric>of(), 0L, i, MetricType.COUNTER));
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
  public void testTags() throws Exception {
    AggregatesTable aggregatesTable = getTableFactory().createAggregates();

    try {
      // Insert 10 metrics, each with 20 tags.
      for (int i = 0; i < 10; i++) {
        List<TagMetric> tags = Lists.newArrayList();
        for (int j = 10; j < 30; j++) {
          tags.add(new TagMetric("tag." + (j / 10) + "." + (j % 10), j));
        }

        aggregatesTable.update(ImmutableList.of(
          new MetricsRecord("context." + i, "runId", "metric", tags, 0L, i, MetricType.COUNTER)
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
  public void testDeleteContext() throws Exception {
    AggregatesTable aggregatesTable = getTableFactory().createAggregates();

    try {
      List<TagMetric> tags = Lists.newArrayList();
      tags.add(new TagMetric("tag.1", 5));
      tags.add(new TagMetric("tag.2", 10));

      aggregatesTable.update(ImmutableList.of(
        // context, runId, metric, tags, timestamp, value
        new MetricsRecord("context.0", "0", "metric.0", tags, 0L, 100, MetricType.COUNTER),
        new MetricsRecord("context.0", "0", "metric.1", tags, 0L, 100, MetricType.COUNTER),
        new MetricsRecord("context.1", "0", "metric.0", tags, 0L, 100, MetricType.COUNTER),
        new MetricsRecord("context.1", "0", "metric.1", tags, 0L, 100, MetricType.COUNTER),
        new MetricsRecord("context.2", "0", "metric.0", tags, 0L, 100, MetricType.COUNTER),
        new MetricsRecord("context.2", "0", "metric.1", tags, 0L, 100, MetricType.COUNTER)
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
  public void testDeleteContextAndMetric() throws Exception {
    AggregatesTable aggregatesTable = getTableFactory().createAggregates();

    try {
      List<TagMetric> tags = Lists.newArrayList();
      tags.add(new TagMetric("tag.1", 5));
      tags.add(new TagMetric("tag.2", 10));

      aggregatesTable.update(ImmutableList.of(
        // context, runId, metric, tags, timestamp, value
        new MetricsRecord("context.0", "0", "metric.0", tags, 0L, 100, MetricType.COUNTER),
        new MetricsRecord("context.0", "0", "metric.1", tags, 0L, 100, MetricType.COUNTER),
        new MetricsRecord("context.1", "0", "metric.0", tags, 0L, 100, MetricType.COUNTER),
        new MetricsRecord("context.1", "0", "metric.1", tags, 0L, 100, MetricType.COUNTER),
        new MetricsRecord("context.2", "0", "metric.0", tags, 0L, 100, MetricType.COUNTER),
        new MetricsRecord("context.2", "0", "metric.1", tags, 0L, 100, MetricType.COUNTER)
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
  public void testDeletes() throws Exception {
    AggregatesTable aggregatesTable = getTableFactory().createAggregates();

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
          new MetricsRecord("context." + i, runId, metric, tags, 0L, 100, MetricType.COUNTER)
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
}
