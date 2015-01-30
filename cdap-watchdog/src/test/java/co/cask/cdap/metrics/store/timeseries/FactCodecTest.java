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

package co.cask.cdap.metrics.store.timeseries;

import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data2.dataset2.lib.table.FuzzyRowFilter;
import co.cask.cdap.data2.dataset2.lib.table.MetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryMetricsTable;
import co.cask.cdap.data2.dataset2.lib.table.inmemory.InMemoryOrderedTableService;
import co.cask.cdap.metrics.data.EntityTable;
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
    InMemoryOrderedTableService.create("FactCodecTest");
    MetricsTable table = new InMemoryMetricsTable("FactCodecTest");
    int resolution = 10;
    int rollTimebaseInterval = 2;
    FactCodec codec = new FactCodec(new EntityTable(table), resolution, rollTimebaseInterval);

    // testing encoding with multiple tags
    List<TagValue> tagValues = ImmutableList.of(new TagValue("tag1", "value1"),
                                                new TagValue("tag2", "value2"),
                                                new TagValue("tag3", "value3"));
    // note: we use seconds everywhere and rely on this
    long ts = 1422312915;
    byte[] rowKey = codec.createRowKey(tagValues, "myMetric", ts);
    byte[] column = codec.createColumn(ts);

    Assert.assertEquals((ts / resolution) * resolution, codec.getTimestamp(rowKey, column));
    Assert.assertEquals(tagValues, codec.getTagValues(rowKey));
    Assert.assertEquals("myMetric", codec.getMeasureName(rowKey));

    // testing encoding without one tag
    tagValues = ImmutableList.of(new TagValue("myTag", "myValue"));
    rowKey = codec.createRowKey(tagValues, "mySingleTagMetric", ts);
    Assert.assertEquals((ts / resolution) * resolution, codec.getTimestamp(rowKey, column));
    Assert.assertEquals(tagValues, codec.getTagValues(rowKey));
    Assert.assertEquals("mySingleTagMetric", codec.getMeasureName(rowKey));

    // testing encoding without empty tags
    rowKey = codec.createRowKey(new ArrayList<TagValue>(), "myNoTagsMetric", ts);
    Assert.assertEquals((ts / resolution) * resolution, codec.getTimestamp(rowKey, column));
    Assert.assertEquals(new ArrayList<TagValue>(), codec.getTagValues(rowKey));
    Assert.assertEquals("myNoTagsMetric", codec.getMeasureName(rowKey));

    // testing null metric
    tagValues = ImmutableList.of(new TagValue("myTag", "myValue"));
    rowKey = codec.createRowKey(tagValues, "mySingleTagMetric", ts);
    Assert.assertEquals((ts / resolution) * resolution, codec.getTimestamp(rowKey, column));
    Assert.assertEquals(tagValues, codec.getTagValues(rowKey));
    Assert.assertEquals("mySingleTagMetric", codec.getMeasureName(rowKey));

    // testing null value
    tagValues = ImmutableList.of(new TagValue("tag1", "value1"),
                                 new TagValue("tag2", null),
                                 new TagValue("tag3", "value3"));
    rowKey = codec.createRowKey(tagValues, "myNullTagMetric", ts);
    Assert.assertEquals((ts / resolution) * resolution, codec.getTimestamp(rowKey, column));
    Assert.assertEquals(tagValues, codec.getTagValues(rowKey));
    Assert.assertEquals("myNullTagMetric", codec.getMeasureName(rowKey));

    // testing fuzzy mask for fuzzy stuff in row key
    tagValues = ImmutableList.of(new TagValue("tag1", "value1"),
                                 new TagValue("tag2", null), // any value is accepted
                                 new TagValue("tag3", "value3"));
    byte[] mask = codec.createFuzzyRowMask(tagValues, "myMetric");
    rowKey = codec.createRowKey(tagValues, "myMetric", ts);
    FuzzyRowFilter filter = new FuzzyRowFilter(ImmutableList.of(new ImmutablePair<byte[], byte[]>(rowKey, mask)));

    tagValues = ImmutableList.of(new TagValue("tag1", "value1"),
                                 new TagValue("tag2", "annnnnnnnnny"),
                                 new TagValue("tag3", "value3"));
    rowKey = codec.createRowKey(tagValues, "myMetric", ts);
    Assert.assertEquals(FuzzyRowFilter.ReturnCode.INCLUDE, filter.filterRow(rowKey));

    tagValues = ImmutableList.of(new TagValue("tag1", "value12"),
                                 new TagValue("tag2", "value2"),
                                 new TagValue("tag3", "value3"));
    rowKey = codec.createRowKey(tagValues, "myMetric", ts);
    Assert.assertTrue(FuzzyRowFilter.ReturnCode.INCLUDE != filter.filterRow(rowKey));

    tagValues = ImmutableList.of(new TagValue("tag1", "value1"),
                                 new TagValue("tag2", "value2"),
                                 new TagValue("tag3", "value13"));
    rowKey = codec.createRowKey(tagValues, "myMetric", ts);
    Assert.assertTrue(FuzzyRowFilter.ReturnCode.INCLUDE != filter.filterRow(rowKey));

    tagValues = ImmutableList.of(new TagValue("tag1", "value1"),
                                 new TagValue("tag3", "value3"));
    rowKey = codec.createRowKey(tagValues, "myMetric", ts);
    Assert.assertTrue(FuzzyRowFilter.ReturnCode.INCLUDE != filter.filterRow(rowKey));

    tagValues = ImmutableList.of(new TagValue("tag1", "value1"),
                                 new TagValue("tag2", "value2"),
                                 new TagValue("tag3", "value3"));
    rowKey = codec.createRowKey(tagValues, "myMetric2", ts);
    Assert.assertTrue(FuzzyRowFilter.ReturnCode.INCLUDE != filter.filterRow(rowKey));

    rowKey = codec.createRowKey(tagValues, null, ts);
    Assert.assertTrue(FuzzyRowFilter.ReturnCode.INCLUDE != filter.filterRow(rowKey));

    rowKey = codec.createRowKey(new ArrayList<TagValue>(), "myMetric", ts);
    Assert.assertTrue(FuzzyRowFilter.ReturnCode.INCLUDE != filter.filterRow(rowKey));

    // testing fuzzy mask for fuzzy metric
    tagValues = ImmutableList.of(new TagValue("myTag", "myValue"));
    rowKey = codec.createRowKey(tagValues, null, ts);
    mask = codec.createFuzzyRowMask(tagValues, null);
    filter = new FuzzyRowFilter(ImmutableList.of(new ImmutablePair<byte[], byte[]>(rowKey, mask)));

    rowKey = codec.createRowKey(tagValues, "annyyy", ts);
    Assert.assertEquals(FuzzyRowFilter.ReturnCode.INCLUDE, filter.filterRow(rowKey));

    rowKey = codec.createRowKey(tagValues, "zzzzzzzzzzzz", ts);
    Assert.assertEquals(FuzzyRowFilter.ReturnCode.INCLUDE, filter.filterRow(rowKey));

    tagValues = ImmutableList.of(new TagValue("myTag", "myValue2"));
    rowKey = codec.createRowKey(tagValues, "metric", ts);
    Assert.assertTrue(FuzzyRowFilter.ReturnCode.INCLUDE != filter.filterRow(rowKey));


    // todo: test prefix of multi tag valued row key is not same one tag valued row key
    // todo: test that rollTimebaseInterval applies well
  }
}
