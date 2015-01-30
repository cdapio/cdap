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
package co.cask.cdap.metrics.store.timeseries;

import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryMetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryOrderedTableService;
import co.cask.cdap.metrics.data.EntityTable;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Test base for {@link co.cask.cdap.metrics.data.TimeSeriesTable}.
 */
public class FactTableTest {

  @Test
  public void testBasics() throws Exception {
    InMemoryOrderedTableService.create("EntityTable");
    InMemoryOrderedTableService.create("DataTable");
    int resolution = 10;
    int rollTimebaseInterval = 2;

    FactTable table = new FactTable(new InMemoryMetricsTable("DataTable"),
                                                            new EntityTable(new InMemoryMetricsTable("EntityTable")),
                                                            resolution, rollTimebaseInterval);

    // aligned to start of resolution bucket
    // "/1000" because time is expected to be in seconds
    long ts = ((System.currentTimeMillis() / 1000) / resolution) * resolution;

    // testing encoding with multiple tags
    List<TagValue> tagValues = ImmutableList.of(new TagValue("tag1", "value1"),
                                                new TagValue("tag2", "value2"),
                                                new TagValue("tag3", "value3"));


    // trying adding one by one, in same (first) time resolution bucket
    for (int i = 0; i < 5; i++) {
      for (int k = 1; k < 4; k++) {
        table.add(ImmutableList.of(new Fact(tagValues, MeasureType.COUNTER, "metric" + k,
                                            // note: "+i" here and below doesn't affect results, just to confirm
                                            //       that data points are rounded to the resolution
                                            new TimeValue(ts + i, k))));
      }
    }

    // trying adding one by one, in different time resolution buckets
    for (int i = 0; i < 3; i++) {
      for (int k = 1; k < 4; k++) {
        table.add(ImmutableList.of(new Fact(tagValues, MeasureType.COUNTER, "metric" + k,
                                            new TimeValue(ts + resolution * i + i, 2 * k))));
      }
    }

    // trying adding as list
    // first incs in same (second) time resolution bucket
    List<Fact> aggs = Lists.newArrayList();
    for (int i = 0; i < 7; i++) {
      for (int k = 1; k < 4; k++) {
        aggs.add(new Fact(tagValues, MeasureType.COUNTER, "metric" + k, new TimeValue(ts + resolution, 3 * k)));
      }
    }
    // then incs in different time resolution buckets
    for (int i = 0; i < 3; i++) {
      for (int k = 1; k < 4; k++) {
        aggs.add(new Fact(tagValues, MeasureType.COUNTER, "metric" + k, new TimeValue(ts + resolution * i, 4 * k)));
      }
    }

    table.add(aggs);

    // verify each metric
    for (int k = 1; k < 4; k++) {
      FactScan scan = new FactScan(ts - 2 * resolution, ts + 3 * resolution, "metric" + k, tagValues);
      Table<String, List<TagValue>, List<TimeValue>> expected = HashBasedTable.create();
      expected.put("metric" + k, tagValues, ImmutableList.of(new TimeValue(ts, 11 * k),
                                                             new TimeValue(ts + resolution, 27 * k),
                                                             new TimeValue(ts + 2 * resolution, 6 * k)));
      assertScan(table, expected, scan);
    }

    // verify all metrics with fuzzy metric in scan
    Table<String, List<TagValue>, List<TimeValue>> expected = HashBasedTable.create();
    for (int k = 1; k < 4; k++) {
      expected.put("metric" + k, tagValues, ImmutableList.of(new TimeValue(ts, 11 * k),
                                                             new TimeValue(ts + resolution, 27 * k),
                                                             new TimeValue(ts + 2 * resolution, 6 * k)));
    }
    // metric = null means "all"
    FactScan scan = new FactScan(ts - 2 * resolution, ts + 3 * resolution, null, tagValues);
    assertScan(table, expected, scan);
  }


  @Test
  public void testQuery() throws Exception {
    InMemoryOrderedTableService.create("QueryEntityTable");
    InMemoryOrderedTableService.create("QueryDataTable");
    int resolution = 10;
    int rollTimebaseInterval = 2;

    FactTable table = new FactTable(new InMemoryMetricsTable("QueryDataTable"),
                                    new EntityTable(new InMemoryMetricsTable("QueryEntityTable")),
                                    resolution, rollTimebaseInterval);

    // aligned to start of resolution bucket
    // "/1000" because time is expected to be in seconds
    long ts = ((System.currentTimeMillis() / 1000) / resolution) * resolution;

    for (int i = 0; i < 3; i++) {
      for (int k = 1; k < 3; k++) {
        // note: "+i" to ts here and below doesn't affect results, just to confirm
        //       that data points are rounded to the resolution
        writeInc(table, "metric" + k, ts + i * resolution + i, k + i, "tag1", "value1", "tag2", "value2");
        writeInc(table, "metric" + k, ts + i * resolution + i, 2 * k + i, "tag1", "value2", "tag2", "value2");
        writeInc(table, "metric" + k, ts + i * resolution + i, 3 * k + i, "tag1", "value2", "tag2", "value1");
        writeInc(table, "metric" + k, ts + i * resolution + i, 4 * k + i, "tag1", "value1", "tag2", "value3");
        // null value in tag matches only fuzzy ("any")
        writeInc(table, "metric" + k, ts + i * resolution + i, 5 * k + i, "tag1", null, "tag2", "value3");
      }
    }

    Table<String, List<TagValue>, List<TimeValue>> expected;
    FactScan scan;

    // simple single metric scan

    for (int i = 1; i < 3; i++) {
      // all time points
      scan = new FactScan(ts - resolution, ts + 3 * resolution,
                            "metric" + i, tagValues("tag1", "value1", "tag2", "value2"));

      expected = HashBasedTable.create();
      expected.put("metric" + i, tagValues("tag1", "value1", "tag2", "value2"),
                   timeValues(ts, resolution, i, i + 1, i + 2));

      assertScan(table, expected, scan);

      // time points since second interval
      scan = new FactScan(ts + resolution, ts + 3 * resolution,
                            "metric" + i, tagValues("tag1", "value1", "tag2", "value2"));

      expected = HashBasedTable.create();
      expected.put("metric" + i, tagValues("tag1", "value1", "tag2", "value2"),
                   timeValues(ts + resolution, resolution, i + 1, i + 2));

      assertScan(table, expected, scan);

      // time points before third interval
      scan = new FactScan(ts - resolution, ts + resolution,
                            "metric" + i, tagValues("tag1", "value1", "tag2", "value2"));

      expected = HashBasedTable.create();
      expected.put("metric" + i, tagValues("tag1", "value1", "tag2", "value2"),
                   timeValues(ts, resolution, i, i + 1));

      assertScan(table, expected, scan);

      // time points for fuzzy tag2 since second interval
      scan = new FactScan(ts + resolution, ts + 3 * resolution,
                            // null stands for any
                            "metric" + i, tagValues("tag1", "value1", "tag2", null));

      expected = HashBasedTable.create();
      expected.put("metric" + i, tagValues("tag1", "value1", "tag2", "value2"),
                   timeValues(ts + resolution, resolution, i + 1, i + 2));
      expected.put("metric" + i, tagValues("tag1", "value1", "tag2", "value3"),
                   timeValues(ts + resolution, resolution, 4 * i + 1, 4 * i + 2));

      assertScan(table, expected, scan);

      // time points for fuzzy tag1 before third interval (very important case - caught some bugs)
      scan = new FactScan(ts - resolution, ts + resolution,
                            // null stands for any
                            "metric" + i, tagValues("tag1", null, "tag2", "value3"));

      expected = HashBasedTable.create();
      expected.put("metric" + i, tagValues("tag1", "value1", "tag2", "value3"),
                   timeValues(ts, resolution, 4 * i, 4 * i + 1));
      expected.put("metric" + i, tagValues("tag1", null, "tag2", "value3"),
                   timeValues(ts, resolution, 5 * i, 5 * i + 1));

      assertScan(table, expected, scan);
    }

    // all time points
    scan = new FactScan(ts - resolution, ts + 3 * resolution,
                          null, tagValues("tag1", "value1", "tag2", "value2"));

    expected = HashBasedTable.create();
    for (int i = 1; i < 3; i++) {
      expected.put("metric" + i, tagValues("tag1", "value1", "tag2", "value2"),
                   timeValues(ts, resolution, i, i + 1, i + 2));
    }

    assertScan(table, expected, scan);

    // time points since second interval
    scan = new FactScan(ts + resolution, ts + 3 * resolution,
                          null, tagValues("tag1", "value1", "tag2", "value2"));

    expected = HashBasedTable.create();
    for (int i = 1; i < 3; i++) {
      expected.put("metric" + i, tagValues("tag1", "value1", "tag2", "value2"),
                   timeValues(ts + resolution, resolution, i + 1, i + 2));
    }

    assertScan(table, expected, scan);

    // time points before third interval
    scan = new FactScan(ts - resolution, ts + resolution,
                          null, tagValues("tag1", "value1", "tag2", "value2"));

    expected = HashBasedTable.create();
    for (int i = 1; i < 3; i++) {
      expected.put("metric" + i, tagValues("tag1", "value1", "tag2", "value2"),
                   timeValues(ts, resolution, i, i + 1));
    }

    assertScan(table, expected, scan);

    // time points for fuzzy tag2 since second interval
    scan = new FactScan(ts + resolution, ts + 3 * resolution,
                          // null stands for any
                          null, tagValues("tag1", "value1", "tag2", null));

    expected = HashBasedTable.create();
    for (int i = 1; i < 3; i++) {
      expected.put("metric" + i, tagValues("tag1", "value1", "tag2", "value2"),
                   timeValues(ts + resolution, resolution, i + 1, i + 2));
      expected.put("metric" + i, tagValues("tag1", "value1", "tag2", "value3"),
                   timeValues(ts + resolution, resolution, 4 * i + 1, 4 * i + 2));
    }

    assertScan(table, expected, scan);

    // time points for fuzzy tag1 before third interval (very important case - caught some bugs)
    scan = new FactScan(ts - resolution, ts + resolution,
                          // null stands for any
                          null, tagValues("tag1", null, "tag2", "value3"));

    expected = HashBasedTable.create();
    for (int i = 1; i < 3; i++) {
      expected.put("metric" + i, tagValues("tag1", "value1", "tag2", "value3"),
                   timeValues(ts, resolution, 4 * i, 4 * i + 1));
      expected.put("metric" + i, tagValues("tag1", null, "tag2", "value3"),
                   timeValues(ts, resolution, 5 * i, 5 * i + 1));
    }

    assertScan(table, expected, scan);
  }

  private List<TimeValue> timeValues(long ts, int resolution, long... values) {
    List<TimeValue> timeValues = Lists.newArrayList();
    for (int i = 0; i < values.length; i++) {
      timeValues.add(new TimeValue(ts + i * resolution, values[i]));
    }
    return timeValues;
  }

  private void writeInc(FactTable table, String metric, long ts, int value, String... tags)
    throws Exception {

    table.add(ImmutableList.of(new Fact(tagValues(tags), MeasureType.COUNTER, metric, new TimeValue(ts, value))));
  }

  private List<TagValue> tagValues(String... tags) {
    List<TagValue> tagValues = Lists.newArrayList();
    for (int i = 0; i < tags.length; i += 2) {
      tagValues.add(new TagValue(tags[i], tags[i + 1]));
    }
    return tagValues;
  }

  private void assertScan(FactTable table,
                          Table<String, List<TagValue>, List<TimeValue>> expected, FactScan scan) throws Exception {
    Table<String, List<TagValue>, List<TimeValue>> resultTable = HashBasedTable.create();
    FactScanner scanner = table.scan(scan);
    try {
      while (scanner.hasNext()) {
        FactScanResult result = scanner.next();
        List<TimeValue> timeValues = resultTable.get(result.getMeasureName(), result.getTagValues());
        if (timeValues == null) {
          timeValues = Lists.newArrayList();
          resultTable.put(result.getMeasureName(), result.getTagValues(), timeValues);
        }
        timeValues.addAll(Lists.newArrayList(result.iterator()));
      }
    } finally {
      scanner.close();
    }

    Assert.assertEquals(expected, resultTable);
  }
}
