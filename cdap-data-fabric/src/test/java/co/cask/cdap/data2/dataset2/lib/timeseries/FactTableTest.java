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
package co.cask.cdap.data2.dataset2.lib.timeseries;

import co.cask.cdap.api.dataset.lib.cube.DimensionValue;
import co.cask.cdap.api.dataset.lib.cube.MeasureType;
import co.cask.cdap.api.dataset.lib.cube.Measurement;
import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryMetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryTableService;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test base for {@link co.cask.cdap.data2.dataset2.lib.timeseries.FactTable}.
 */
public class FactTableTest {

  @Test
  public void testBasics() throws Exception {
    InMemoryTableService.create("EntityTable");
    InMemoryTableService.create("DataTable");
    int resolution = 10;
    int rollTimebaseInterval = 2;

    FactTable table = new FactTable(new InMemoryMetricsTable("DataTable"),
                                                            new EntityTable(new InMemoryMetricsTable("EntityTable")),
                                                            resolution, rollTimebaseInterval);

    // aligned to start of resolution bucket
    // "/1000" because time is expected to be in seconds
    long ts = ((System.currentTimeMillis() / 1000) / resolution) * resolution;

    // testing encoding with multiple dims
    List<DimensionValue> dimensionValues = ImmutableList.of(new DimensionValue("dim1", "value1"),
                                                new DimensionValue("dim2", "value2"),
                                                new DimensionValue("dim3", "value3"));


    // trying adding one by one, in same (first) time resolution bucket
    for (int i = 0; i < 5; i++) {
      for (int k = 1; k < 4; k++) {
        // note: "+i" here and below doesn't affect results, just to confirm
        //       that data points are rounded to the resolution
        table.add(ImmutableList.of(new Fact(ts + i, dimensionValues,
                                            new Measurement("metric" + k, MeasureType.COUNTER, k))));
      }
    }

    // trying adding one by one, in different time resolution buckets
    for (int i = 0; i < 3; i++) {
      for (int k = 1; k < 4; k++) {
        table.add(ImmutableList.of(new Fact(ts + resolution * i + i, dimensionValues,
                                            new Measurement("metric" + k, MeasureType.COUNTER, 2 * k))));
      }
    }

    // trying adding as list
    // first incs in same (second) time resolution bucket
    List<Fact> aggs = Lists.newArrayList();
    for (int i = 0; i < 7; i++) {
      for (int k = 1; k < 4; k++) {
        aggs.add(new Fact(ts + resolution, dimensionValues, new Measurement("metric" + k, MeasureType.COUNTER, 3 * k)));
      }
    }
    // then incs in different time resolution buckets
    for (int i = 0; i < 3; i++) {
      for (int k = 1; k < 4; k++) {
        aggs.add(new Fact(ts + resolution * i, dimensionValues,
                          new Measurement("metric" + k, MeasureType.COUNTER, 4 * k)));
      }
    }

    table.add(aggs);

    // verify each metric
    for (int k = 1; k < 4; k++) {
      FactScan scan = new FactScan(ts - 2 * resolution, ts + 3 * resolution, "metric" + k, dimensionValues);
      Table<String, List<DimensionValue>, List<TimeValue>> expected = HashBasedTable.create();
      expected.put("metric" + k, dimensionValues, ImmutableList.of(new TimeValue(ts, 11 * k),
                                                             new TimeValue(ts + resolution, 27 * k),
                                                             new TimeValue(ts + 2 * resolution, 6 * k)));
      assertScan(table, expected, scan);
    }

    // verify each metric within a single timeBase
    for (int k = 1; k < 4; k++) {
      FactScan scan = new FactScan(ts, ts + resolution - 1, "metric" + k, dimensionValues);
      Table<String, List<DimensionValue>, List<TimeValue>> expected = HashBasedTable.create();
      expected.put("metric" + k, dimensionValues, ImmutableList.of(new TimeValue(ts, 11 * k)));
      assertScan(table, expected, scan);
    }

    // verify all metrics with fuzzy metric in scan
    Table<String, List<DimensionValue>, List<TimeValue>> expected = HashBasedTable.create();
    for (int k = 1; k < 4; k++) {
      expected.put("metric" + k, dimensionValues, ImmutableList.of(new TimeValue(ts, 11 * k),
                                                             new TimeValue(ts + resolution, 27 * k),
                                                             new TimeValue(ts + 2 * resolution, 6 * k)));
    }
    // metric = null means "all"
    FactScan scan = new FactScan(ts - 2 * resolution, ts + 3 * resolution, dimensionValues);
    assertScan(table, expected, scan);

    // delete metric test
    expected.clear();

    // delete the metrics data at (timestamp + 20) resolution
    scan = new FactScan(ts + resolution * 2, ts + resolution * 3, dimensionValues);
    table.delete(scan);
    for (int k = 1; k < 4; k++) {
      expected.put("metric" + k, dimensionValues, ImmutableList.of(new TimeValue(ts, 11 * k),
                                                             new TimeValue(ts + resolution, 27 * k)));
    }
    // verify deletion
    scan = new FactScan(ts - 2 * resolution, ts + 3 * resolution, dimensionValues);
    assertScan(table, expected, scan);

    // delete metrics for "metric1" at ts0 and verify deletion
    scan = new FactScan(ts, ts + 1, "metric1", dimensionValues);
    table.delete(scan);
    expected.clear();
    expected.put("metric1", dimensionValues, ImmutableList.of(new TimeValue(ts + resolution, 27)));
    scan = new FactScan(ts - 2 * resolution, ts + 3 * resolution, "metric1", dimensionValues);
    assertScan(table, expected, scan);

    // verify the next dims search
    Collection<DimensionValue> nextTags =
      table.findSingleDimensionValue(ImmutableList.of("dim1", "dim2", "dim3"),
                                     ImmutableMap.of("dim1", "value1"), ts, ts + 1);
    Assert.assertEquals(ImmutableSet.of(new DimensionValue("dim2", "value2")), nextTags);

    Map<String, String> slice = Maps.newHashMap();
    slice.put("dim1", null);
    nextTags = table.findSingleDimensionValue(ImmutableList.of("dim1", "dim2", "dim3"), slice, ts, ts + 1);
    Assert.assertEquals(ImmutableSet.of(new DimensionValue("dim2", "value2")), nextTags);

    nextTags = table.findSingleDimensionValue(ImmutableList.of("dim1", "dim2", "dim3"),
                                              ImmutableMap.of("dim1", "value1", "dim2", "value2"), ts, ts + 3);
    Assert.assertEquals(ImmutableSet.of(new DimensionValue("dim3", "value3")), nextTags);

    // add new dim values
    dimensionValues = ImmutableList.of(new DimensionValue("dim1", "value1"),
                                 new DimensionValue("dim2", "value5"),
                                 new DimensionValue("dim3", null));
    table.add(ImmutableList.of(new Fact(ts, dimensionValues, new Measurement("metric", MeasureType.COUNTER, 10))));

    dimensionValues = ImmutableList.of(new DimensionValue("dim1", "value1"),
                                 new DimensionValue("dim2", null),
                                 new DimensionValue("dim3", "value3"));
    table.add(ImmutableList.of(new Fact(ts, dimensionValues, new Measurement("metric", MeasureType.COUNTER, 10))));

    nextTags = table.findSingleDimensionValue(ImmutableList.of("dim1", "dim2", "dim3"),
                                              ImmutableMap.of("dim1", "value1"), ts, ts + 1);
    Assert.assertEquals(ImmutableSet.of(new DimensionValue("dim2", "value2"),
                                        new DimensionValue("dim2", "value5"),
                                        new DimensionValue("dim3", "value3")), nextTags);

    // search for metric names given dims list and verify

    Collection<String> metricNames =
      table.findMeasureNames(ImmutableList.of("dim1", "dim2", "dim3"),
                             ImmutableMap.of("dim1", "value1", "dim2", "value2", "dim3", "value3"), ts, ts + 1);
    Assert.assertEquals(ImmutableSet.of("metric2", "metric3"), metricNames);

    metricNames = table.findMeasureNames(ImmutableList.of("dim1", "dim2", "dim3"),
                                         ImmutableMap.of("dim1", "value1"), ts, ts + 1);
    Assert.assertEquals(ImmutableSet.of("metric", "metric2", "metric3"), metricNames);

    metricNames = table.findMeasureNames(ImmutableList.of("dim1", "dim2", "dim3"),
                                         ImmutableMap.of("dim2", "value2"), ts, ts + 1);
    Assert.assertEquals(ImmutableSet.of("metric2", "metric3"), metricNames);

    metricNames = table.findMeasureNames(ImmutableList.of("dim1", "dim2", "dim3"),
                                         slice, ts, ts + 1);
    Assert.assertEquals(ImmutableSet.of("metric", "metric2", "metric3"), metricNames);
  }

  @Test
  public void testSearch() throws Exception {
    InMemoryTableService.create("SearchEntityTable");
    InMemoryTableService.create("SearchDataTable");
    int resolution = Integer.MAX_VALUE;
    int rollTimebaseInterval = 2;

    FactTable table = new FactTable(new InMemoryMetricsTable("SearchDataTable"),
                                    new EntityTable(new InMemoryMetricsTable("SearchEntityTable")),
                                    resolution, rollTimebaseInterval);

    // aligned to start of resolution bucket
    // "/1000" because time is expected to be in seconds
    long ts = ((System.currentTimeMillis() / 1000) / resolution) * resolution;
    List<String> aggregationList = ImmutableList.of("dim1", "dim2", "dim3", "dim4");

    for (int i = 0; i < 2; i++) {
        writeInc(table, "metric-a" + i, ts  + i,  i,
                 "dim1", "value1", "dim2", "value2", "dim3", "value3", "dim4", "value4");
        writeInc(table, "metric-b" + i, ts  + i,  i,
                 "dim1", "value2", "dim2", "value2", "dim3", "x3", "dim4", "x4");
        writeInc(table, "metric-c" + i, ts  + i,  i,
                 "dim1", "value2", "dim2", "value2", "dim3", null, "dim4", "y4");
        writeInc(table, "metric-d" + i, ts + i,  i,
                 "dim1", "value1", "dim2", "value3", "dim3", "y3", "dim4", null);
    }

    Map<String, String> slice = Maps.newHashMap();
    slice.put("dim1", "value2");
    slice.put("dim2", "value2");
    slice.put("dim3", null);
    // verify search dims
    testTagSearch(table, aggregationList, ImmutableMap.of("dim2", "value2"),
                  ImmutableSet.of(new DimensionValue("dim1", "value1"), new DimensionValue("dim1", "value2")));

    testTagSearch(table, aggregationList, ImmutableMap.of("dim1", "value1"),
                  ImmutableSet.of(new DimensionValue("dim2", "value2"), new DimensionValue("dim2", "value3")));

    testTagSearch(table, aggregationList, ImmutableMap.<String, String>of(),
                  ImmutableSet.of(new DimensionValue("dim1", "value1"), new DimensionValue("dim1", "value2")));

    testTagSearch(table, aggregationList, ImmutableMap.of("dim1", "value2", "dim2", "value2"),
                  ImmutableSet.of(new DimensionValue("dim3", "x3"), new DimensionValue("dim4", "y4")));

    testTagSearch(table, aggregationList, slice,
                  ImmutableSet.of(new DimensionValue("dim4", "x4"), new DimensionValue("dim4", "y4")));

    testTagSearch(table, aggregationList, ImmutableMap.of("dim1", "value2", "dim2", "value3", "dim3", "y3"),
                  ImmutableSet.<DimensionValue>of());

    // verify search metrics

    testMetricNamesSearch(table, aggregationList, ImmutableMap.of("dim1", "value1", "dim2", "value2", "dim3", "value3"),
                          ImmutableSet.<String>of("metric-a0", "metric-a1"));

    testMetricNamesSearch(table, aggregationList, ImmutableMap.of("dim2", "value2"),
                          ImmutableSet.<String>of("metric-a0", "metric-a1", "metric-b0", "metric-b1",
                                                  "metric-c0", "metric-c1"));

    testMetricNamesSearch(table, aggregationList, slice, ImmutableSet.of("metric-b0", "metric-b1",
                                                  "metric-c0", "metric-c1"));

  }

  private void testMetricNamesSearch(FactTable table, List<String> aggregationList ,
                                     Map<String, String> sliceBy,
                                     ImmutableSet<String> expectedResuls) throws Exception {
    Collection<String> metricNames =
      table.findMeasureNames(aggregationList , sliceBy, 0, 1);
    Assert.assertEquals(expectedResuls, metricNames);
  }

  private void testTagSearch(FactTable table, List<String> aggregation, Map<String, String> sliceBy,
                             Set<DimensionValue> expectedResult) throws Exception {
    // using 0, 1 as start and endTs as resolution is INT_MAX
    Collection<DimensionValue> nextTags =
      table.findSingleDimensionValue(aggregation, sliceBy, 0, 1);
    Assert.assertEquals(expectedResult, nextTags);
  }
  @Test
  public void testQuery() throws Exception {
    InMemoryTableService.create("QueryEntityTable");
    InMemoryTableService.create("QueryDataTable");
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
        writeInc(table, "metric" + k, ts + i * resolution + i, k + i, "dim1", "value1", "dim2", "value2");
        writeInc(table, "metric" + k, ts + i * resolution + i, 2 * k + i, "dim1", "value2", "dim2", "value2");
        writeInc(table, "metric" + k, ts + i * resolution + i, 3 * k + i, "dim1", "value2", "dim2", "value1");
        writeInc(table, "metric" + k, ts + i * resolution + i, 4 * k + i, "dim1", "value1", "dim2", "value3");
        // null value in dim matches only fuzzy ("any")
        writeInc(table, "metric" + k, ts + i * resolution + i, 5 * k + i, "dim1", null, "dim2", "value3");
      }
    }

    Table<String, List<DimensionValue>, List<TimeValue>> expected;
    FactScan scan;

    // simple single metric scan

    for (int i = 1; i < 3; i++) {
      // all time points
      scan = new FactScan(ts - resolution, ts + 3 * resolution,
                          "metric" + i, dimValues("dim1", "value1", "dim2", "value2"));

      expected = HashBasedTable.create();
      expected.put("metric" + i, dimValues("dim1", "value1", "dim2", "value2"),
                   timeValues(ts, resolution, i, i + 1, i + 2));

      assertScan(table, expected, scan);

      // time points since second interval
      scan = new FactScan(ts + resolution, ts + 3 * resolution,
                          "metric" + i, dimValues("dim1", "value1", "dim2", "value2"));

      expected = HashBasedTable.create();
      expected.put("metric" + i, dimValues("dim1", "value1", "dim2", "value2"),
                   timeValues(ts + resolution, resolution, i + 1, i + 2));

      assertScan(table, expected, scan);

      // time points before third interval
      scan = new FactScan(ts - resolution, ts + resolution,
                          "metric" + i, dimValues("dim1", "value1", "dim2", "value2"));

      expected = HashBasedTable.create();
      expected.put("metric" + i, dimValues("dim1", "value1", "dim2", "value2"),
                   timeValues(ts, resolution, i, i + 1));

      assertScan(table, expected, scan);

      // time points for fuzzy dim2 since second interval
      scan = new FactScan(ts + resolution, ts + 3 * resolution,
                            // null stands for any
                            "metric" + i, dimValues("dim1", "value1", "dim2", null));

      expected = HashBasedTable.create();
      expected.put("metric" + i, dimValues("dim1", "value1", "dim2", "value2"),
                   timeValues(ts + resolution, resolution, i + 1, i + 2));
      expected.put("metric" + i, dimValues("dim1", "value1", "dim2", "value3"),
                   timeValues(ts + resolution, resolution, 4 * i + 1, 4 * i + 2));

      assertScan(table, expected, scan);

      // time points for fuzzy dim1 before third interval
      scan = new FactScan(ts - resolution, ts + resolution,
                            // null stands for any
                            "metric" + i, dimValues("dim1", null, "dim2", "value3"));

      expected = HashBasedTable.create();
      expected.put("metric" + i, dimValues("dim1", "value1", "dim2", "value3"),
                   timeValues(ts, resolution, 4 * i, 4 * i + 1));
      expected.put("metric" + i, dimValues("dim1", null, "dim2", "value3"),
                   timeValues(ts, resolution, 5 * i, 5 * i + 1));

      assertScan(table, expected, scan);

      // time points for both fuzzy dims before third interval
      scan = new FactScan(ts - resolution, ts + resolution,
                          // null stands for any
                          "metric" + i, dimValues("dim1", null, "dim2", null));

      expected = HashBasedTable.create();
      expected.put("metric" + i, dimValues("dim1", "value1", "dim2", "value2"),
                   timeValues(ts, resolution, i, i + 1));
      expected.put("metric" + i, dimValues("dim1", "value2", "dim2", "value1"),
                   timeValues(ts, resolution, 3 * i, 3 * i + 1));
      expected.put("metric" + i, dimValues("dim1", "value2", "dim2", "value2"),
                   timeValues(ts, resolution, 2 * i, 2 * i + 1));
      expected.put("metric" + i, dimValues("dim1", "value1", "dim2", "value3"),
                   timeValues(ts, resolution, 4 * i, 4 * i + 1));
      expected.put("metric" + i, dimValues("dim1", null, "dim2", "value3"),
                   timeValues(ts, resolution, 5 * i, 5 * i + 1));

      assertScan(table, expected, scan);

      // time points for both fuzzy dims since third interval
      scan = new FactScan(ts + resolution, ts + 3 * resolution,
                          // null stands for any
                          "metric" + i, dimValues("dim1", null, "dim2", null));

      expected = HashBasedTable.create();
      expected.put("metric" + i, dimValues("dim1", "value1", "dim2", "value2"),
                   timeValues(ts + resolution, resolution, i + 1, i + 2));
      expected.put("metric" + i, dimValues("dim1", "value2", "dim2", "value1"),
                   timeValues(ts + resolution, resolution, 3 * i + 1, 3 * i + 2));
      expected.put("metric" + i, dimValues("dim1", "value2", "dim2", "value2"),
                   timeValues(ts + resolution, resolution, 2 * i + 1, 2 * i + 2));
      expected.put("metric" + i, dimValues("dim1", "value1", "dim2", "value3"),
                   timeValues(ts + resolution, resolution, 4 * i + 1, 4 * i + 2));
      expected.put("metric" + i, dimValues("dim1", null, "dim2", "value3"),
                   timeValues(ts + resolution, resolution, 5 * i + 1, 5 * i + 2));

      assertScan(table, expected, scan);
    }

    // all time points
    scan = new FactScan(ts - resolution, ts + 3 * resolution, dimValues("dim1", "value1", "dim2", "value2"));

    expected = HashBasedTable.create();
    for (int i = 1; i < 3; i++) {
      expected.put("metric" + i, dimValues("dim1", "value1", "dim2", "value2"),
                   timeValues(ts, resolution, i, i + 1, i + 2));
    }

    assertScan(table, expected, scan);

    // time points since second interval
    scan = new FactScan(ts + resolution, ts + 3 * resolution, dimValues("dim1", "value1", "dim2", "value2"));

    expected = HashBasedTable.create();
    for (int i = 1; i < 3; i++) {
      expected.put("metric" + i, dimValues("dim1", "value1", "dim2", "value2"),
                   timeValues(ts + resolution, resolution, i + 1, i + 2));
    }

    assertScan(table, expected, scan);

    // time points before third interval
    scan = new FactScan(ts - resolution, ts + resolution, dimValues("dim1", "value1", "dim2", "value2"));

    expected = HashBasedTable.create();
    for (int i = 1; i < 3; i++) {
      expected.put("metric" + i, dimValues("dim1", "value1", "dim2", "value2"),
                   timeValues(ts, resolution, i, i + 1));
    }

    assertScan(table, expected, scan);

    // time points for fuzzy dim2 since second interval
    scan = new FactScan(ts + resolution, ts + 3 * resolution, dimValues("dim1", "value1", "dim2", null));

    expected = HashBasedTable.create();
    for (int i = 1; i < 3; i++) {
      expected.put("metric" + i, dimValues("dim1", "value1", "dim2", "value2"),
                   timeValues(ts + resolution, resolution, i + 1, i + 2));
      expected.put("metric" + i, dimValues("dim1", "value1", "dim2", "value3"),
                   timeValues(ts + resolution, resolution, 4 * i + 1, 4 * i + 2));
    }

    assertScan(table, expected, scan);

    // time points for fuzzy dim1 before third interval (very important case - caught some bugs)
    scan = new FactScan(ts - resolution, ts + resolution, dimValues("dim1", null, "dim2", "value3"));

    expected = HashBasedTable.create();
    for (int i = 1; i < 3; i++) {
      expected.put("metric" + i, dimValues("dim1", "value1", "dim2", "value3"),
                   timeValues(ts, resolution, 4 * i, 4 * i + 1));
      expected.put("metric" + i, dimValues("dim1", null, "dim2", "value3"),
                   timeValues(ts, resolution, 5 * i, 5 * i + 1));
    }

    assertScan(table, expected, scan);
  }

  @Test
  public void testMaxResolution() throws Exception {
    // we use Integer.MAX_VALUE as resolution to compute all-time total values
    InMemoryTableService.create("TotalsEntityTable");
    InMemoryTableService.create("TotalsDataTable");
    int resolution = Integer.MAX_VALUE;
    // should not matter when resolution is max
    int rollTimebaseInterval = 3600;

    FactTable table = new FactTable(new InMemoryMetricsTable("TotalsDataTable"),
                                    new EntityTable(new InMemoryMetricsTable("TotalsEntityTable")),
                                    resolution, rollTimebaseInterval);


    // ts is expected in seconds
    long ts = System.currentTimeMillis() / 1000;
    int count = 1000;
    for (int i = 0; i < count; i++) {
      for (int k = 0; k < 10; k++) {
        // shift one day
        writeInc(table, "metric" + k, ts + i * 60 * 60 * 24, i * k, "dim" + k, "value" + k);
      }
    }

    for (int k = 0; k < 10; k++) {
      // 0, 0 should match timestamp of all data points
      FactScan scan = new FactScan(0, 0, "metric" + k, dimValues("dim" + k, "value" + k));

      Table<String, List<DimensionValue>, List<TimeValue>> expected = HashBasedTable.create();
      expected.put("metric" + k, dimValues("dim" + k, "value" + k),
                   ImmutableList.of(new TimeValue(0, k * count * (count - 1) / 2)));

      assertScan(table, expected, scan);
    }
  }

  private List<TimeValue> timeValues(long ts, int resolution, long... values) {
    List<TimeValue> timeValues = Lists.newArrayList();
    for (int i = 0; i < values.length; i++) {
      timeValues.add(new TimeValue(ts + i * resolution, values[i]));
    }
    return timeValues;
  }

  private void writeInc(FactTable table, String metric, long ts, int value, String... dims)
    throws Exception {

    table.add(ImmutableList.of(new Fact(ts, dimValues(dims), new Measurement(metric, MeasureType.COUNTER, value))));
  }

  private List<DimensionValue> dimValues(String... dims) {
    List<DimensionValue> dimensionValues = Lists.newArrayList();
    for (int i = 0; i < dims.length; i += 2) {
      dimensionValues.add(new DimensionValue(dims[i], dims[i + 1]));
    }
    return dimensionValues;
  }

  private void assertScan(FactTable table, Table<String, List<DimensionValue>, List<TimeValue>> expected,
                          FactScan scan) throws Exception {
    Table<String, List<DimensionValue>, List<TimeValue>> resultTable = HashBasedTable.create();
    FactScanner scanner = table.scan(scan);
    try {
      while (scanner.hasNext()) {
        FactScanResult result = scanner.next();
        List<TimeValue> timeValues = resultTable.get(result.getMeasureName(), result.getDimensionValues());
        if (timeValues == null) {
          timeValues = Lists.newArrayList();
          resultTable.put(result.getMeasureName(), result.getDimensionValues(), timeValues);
        }
        timeValues.addAll(Lists.newArrayList(result.iterator()));
      }
    } finally {
      scanner.close();
    }

    Assert.assertEquals(expected, resultTable);
  }
}
