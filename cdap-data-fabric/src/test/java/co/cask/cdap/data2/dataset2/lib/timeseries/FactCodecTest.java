/*
 * Copyright 2015 Cask Data, Inc.
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
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryMetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryTableService;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class FactCodecTest {
  @Test
  public void test() {
    InMemoryTableService.create("FactCodecTest");
    MetricsTable table = new InMemoryMetricsTable("FactCodecTest");
    int resolution = 10;
    int rollTimebaseInterval = 2;
    FactCodec codec = new FactCodec(new EntityTable(table), resolution, rollTimebaseInterval);

    // testing encoding with multiple dimensions
    List<DimensionValue> dimensionValues = ImmutableList.of(new DimensionValue("dimension1", "value1"),
                                                new DimensionValue("dimension2", "value2"),
                                                new DimensionValue("dimension3", "value3"));
    // note: we use seconds everywhere and rely on this
    long ts = 1422312915;
    byte[] rowKey = codec.createRowKey(dimensionValues, "myMetric", ts);
    byte[] column = codec.createColumn(ts);

    Assert.assertEquals((ts / resolution) * resolution, codec.getTimestamp(rowKey, column));
    Assert.assertEquals(dimensionValues, codec.getDimensionValues(rowKey));
    Assert.assertEquals("myMetric", codec.getMeasureName(rowKey));

    // testing encoding without one dimension
    dimensionValues = ImmutableList.of(new DimensionValue("myTag", "myValue"));
    rowKey = codec.createRowKey(dimensionValues, "mySingleTagMetric", ts);
    Assert.assertEquals((ts / resolution) * resolution, codec.getTimestamp(rowKey, column));
    Assert.assertEquals(dimensionValues, codec.getDimensionValues(rowKey));
    Assert.assertEquals("mySingleTagMetric", codec.getMeasureName(rowKey));

    // testing encoding without empty dimensions
    rowKey = codec.createRowKey(new ArrayList<DimensionValue>(), "myNoTagsMetric", ts);
    Assert.assertEquals((ts / resolution) * resolution, codec.getTimestamp(rowKey, column));
    Assert.assertEquals(new ArrayList<DimensionValue>(), codec.getDimensionValues(rowKey));
    Assert.assertEquals("myNoTagsMetric", codec.getMeasureName(rowKey));

    // testing null metric
    dimensionValues = ImmutableList.of(new DimensionValue("myTag", "myValue"));
    rowKey = codec.createRowKey(dimensionValues, "mySingleTagMetric", ts);
    Assert.assertEquals((ts / resolution) * resolution, codec.getTimestamp(rowKey, column));
    Assert.assertEquals(dimensionValues, codec.getDimensionValues(rowKey));
    Assert.assertEquals("mySingleTagMetric", codec.getMeasureName(rowKey));

    // testing null value
    dimensionValues = ImmutableList.of(new DimensionValue("dimension1", "value1"),
                                 new DimensionValue("dimension2", null),
                                 new DimensionValue("dimension3", "value3"));
    rowKey = codec.createRowKey(dimensionValues, "myNullTagMetric", ts);
    Assert.assertEquals((ts / resolution) * resolution, codec.getTimestamp(rowKey, column));
    Assert.assertEquals(dimensionValues, codec.getDimensionValues(rowKey));
    Assert.assertEquals("myNullTagMetric", codec.getMeasureName(rowKey));

    // testing fuzzy mask for fuzzy stuff in row key
    dimensionValues = ImmutableList.of(new DimensionValue("dimension1", "value1"),
                                 new DimensionValue("dimension2", null), // any value is accepted
                                 new DimensionValue("dimension3", "value3"));
    byte[] mask = codec.createFuzzyRowMask(dimensionValues, "myMetric");
    rowKey = codec.createRowKey(dimensionValues, "myMetric", ts);
    FuzzyRowFilter filter = new FuzzyRowFilter(ImmutableList.of(new ImmutablePair<byte[], byte[]>(rowKey, mask)));

    dimensionValues = ImmutableList.of(new DimensionValue("dimension1", "value1"),
                                 new DimensionValue("dimension2", "annnnnnnnnny"),
                                 new DimensionValue("dimension3", "value3"));
    rowKey = codec.createRowKey(dimensionValues, "myMetric", ts);
    Assert.assertEquals(FuzzyRowFilter.ReturnCode.INCLUDE, filter.filterRow(rowKey));

    dimensionValues = ImmutableList.of(new DimensionValue("dimension1", "value12"),
                                 new DimensionValue("dimension2", "value2"),
                                 new DimensionValue("dimension3", "value3"));
    rowKey = codec.createRowKey(dimensionValues, "myMetric", ts);
    Assert.assertTrue(FuzzyRowFilter.ReturnCode.INCLUDE != filter.filterRow(rowKey));

    dimensionValues = ImmutableList.of(new DimensionValue("dimension1", "value1"),
                                 new DimensionValue("dimension2", "value2"),
                                 new DimensionValue("dimension3", "value13"));
    rowKey = codec.createRowKey(dimensionValues, "myMetric", ts);
    Assert.assertTrue(FuzzyRowFilter.ReturnCode.INCLUDE != filter.filterRow(rowKey));

    dimensionValues = ImmutableList.of(new DimensionValue("dimension1", "value1"),
                                 new DimensionValue("dimension3", "value3"));
    rowKey = codec.createRowKey(dimensionValues, "myMetric", ts);
    Assert.assertTrue(FuzzyRowFilter.ReturnCode.INCLUDE != filter.filterRow(rowKey));

    // fuzzy in value should match the "null" value
    dimensionValues = ImmutableList.of(new DimensionValue("dimension1", "value1"),
                                 new DimensionValue("dimension2", null),
                                 new DimensionValue("dimension3", "value3"));
    rowKey = codec.createRowKey(dimensionValues, "myMetric", ts);
    Assert.assertEquals(FuzzyRowFilter.ReturnCode.INCLUDE, filter.filterRow(rowKey));

    dimensionValues = ImmutableList.of(new DimensionValue("dimension1", "value1"),
                                 new DimensionValue("dimension2", "value2"),
                                 new DimensionValue("dimension3", "value3"));
    rowKey = codec.createRowKey(dimensionValues, "myMetric2", ts);
    Assert.assertTrue(FuzzyRowFilter.ReturnCode.INCLUDE != filter.filterRow(rowKey));

    rowKey = codec.createRowKey(dimensionValues, null, ts);
    Assert.assertTrue(FuzzyRowFilter.ReturnCode.INCLUDE != filter.filterRow(rowKey));

    rowKey = codec.createRowKey(new ArrayList<DimensionValue>(), "myMetric", ts);
    Assert.assertTrue(FuzzyRowFilter.ReturnCode.INCLUDE != filter.filterRow(rowKey));

    // testing fuzzy mask for fuzzy metric
    dimensionValues = ImmutableList.of(new DimensionValue("myTag", "myValue"));
    rowKey = codec.createRowKey(dimensionValues, null, ts);
    mask = codec.createFuzzyRowMask(dimensionValues, null);
    filter = new FuzzyRowFilter(ImmutableList.of(new ImmutablePair<byte[], byte[]>(rowKey, mask)));

    rowKey = codec.createRowKey(dimensionValues, "annyyy", ts);
    Assert.assertEquals(FuzzyRowFilter.ReturnCode.INCLUDE, filter.filterRow(rowKey));

    rowKey = codec.createRowKey(dimensionValues, "zzzzzzzzzzzz", ts);
    Assert.assertEquals(FuzzyRowFilter.ReturnCode.INCLUDE, filter.filterRow(rowKey));

    dimensionValues = ImmutableList.of(new DimensionValue("myTag", "myValue2"));
    rowKey = codec.createRowKey(dimensionValues, "metric", ts);
    Assert.assertTrue(FuzzyRowFilter.ReturnCode.INCLUDE != filter.filterRow(rowKey));


    // todo: test prefix of multi dimension valued row key is not same one dimension valued row key
    // todo: test that rollTimebaseInterval applies well
  }
}
