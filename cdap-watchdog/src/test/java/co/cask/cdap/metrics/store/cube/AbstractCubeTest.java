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

package co.cask.cdap.metrics.store.cube;

import co.cask.cdap.metrics.store.timeseries.MeasureType;
import co.cask.cdap.metrics.store.timeseries.TimeValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public abstract class AbstractCubeTest {
  protected abstract Cube getCube(String name, int[] resolutions, Collection<? extends Aggregation> aggregations);

  @Test
  public void test() throws Exception {
    Aggregation agg1 = new DefaultAggregation(ImmutableList.of("tag1", "tag2", "tag3"),
                                              ImmutableList.of("tag1", "tag2"));
    Aggregation agg2 = new DefaultAggregation(ImmutableList.of("tag1", "tag2"),
                                              ImmutableList.of("tag1"));

    Cube cube = getCube("myCube", new int[] {1}, ImmutableList.of(agg1, agg2));

    // write some data
    // NOTE: we mostly use different ts, as we are interested in checking incs not at persist, but rather at query time
    writeInc(cube, "metric1",  1,  1,  "1",  "1",  "1");
    writeInc(cube, "metric1",  1,  1,  "1",  "1",  "1");
    writeInc(cube, "metric1",  2,  2, null,  "1",  "1");
    writeInc(cube, "metric1",  3,  3,  "1",  "2",  "1");
    writeInc(cube, "metric1",  3,  5,  "1",  "2",  "3");
    writeInc(cube, "metric1",  3,  7,  "2",  "1",  "1");
    writeInc(cube, "metric1",  4,  4,  "1", null,  "2");
    writeInc(cube, "metric1",  5,  5, null, null,  "1");
    writeInc(cube, "metric1",  6,  6,  "1", null, null);
    writeInc(cube, "metric1",  7,  3,  "1",  "1", null);
    writeInc(cube, "metric1",  8,  2, null,  "1", null);
    writeInc(cube, "metric1",  9,  1, null, null, null);
    writeInc(cube, "metric1", 10,  2,  "1",  "1",  "1",  "1");
    writeInc(cube, "metric1", 11,  3,  "1",  "1",  "1", null);
    writeInc(cube, "metric1", 12,  4,  "2",  "1",  "1",  "1");
    writeInc(cube, "metric1", 13,  5, null, null, null,  "1");

    writeInc(cube, "metric2",  1,  1,  "1",  "1",  "1");


    // now let's try querying
    verifyCountQuery(cube, 0, 15, 1, "metric1", ImmutableMap.of("tag1", "1"), ImmutableList.of("tag2"),
                     ImmutableList.of(
                       new TimeSeries("metric1", tagValuesWithNulls("tag2", null), timeValues(4, 4, 6, 6)),
                       new TimeSeries("metric1", tagValuesWithNulls("tag2", "1"), timeValues(1, 2, 7, 3, 10, 2, 11, 3)),
                       new TimeSeries("metric1", tagValuesWithNulls("tag2", "2"), timeValues(3, 8))));

    verifyCountQuery(cube, 0, 15, 1, "metric1", ImmutableMap.of("tag1", "1", "tag2", "1", "tag3", "1"),
                     new ArrayList<String>(),
                     ImmutableList.of(
                       new TimeSeries("metric1", new HashMap<String, String>(), timeValues(1, 2, 10, 2, 11, 3))));

    verifyCountQuery(cube, 0, 15, 1, "metric1", new HashMap<String, String>(), ImmutableList.of("tag1"),
                     ImmutableList.of(
                       new TimeSeries("metric1", tagValuesWithNulls("tag1", "1"),
                                      timeValues(1, 2, 3, 8, 4, 4, 6, 6, 7, 3, 10, 2, 11, 3)),
                       new TimeSeries("metric1", tagValuesWithNulls("tag1", "2"),
                                      timeValues(3, 7, 12, 4))));

    // now let's query!
    verifyCountQuery(cube, 0, 15, 1, "metric1", ImmutableMap.of("tag3", "3"), new ArrayList<String>(),
                     ImmutableList.of(
                       new TimeSeries("metric1", new HashMap<String, String>(), timeValues(3, 5))));

  }

  private void writeInc(Cube cube, String measureName, long ts, long value, String... tags) throws Exception {
    cube.add(new CubeFact(tagValues(tags), MeasureType.COUNTER, measureName, new TimeValue(ts, value)));
  }

  private Map<String, String> tagValues(String... tags) {
    Map<String, String> tagValues = Maps.newHashMap();
    for (int i = 0; i < tags.length; i++) {
      if (tags[i] != null) {
        // +1 to start with "tag1"
        tagValues.put("tag" + (i + 1), tags[i]);
      }
    }
    return tagValues;
  }

  private void verifyCountQuery(Cube cube, long startTs, long endTs, int resolution, String measureName,
                                Map<String, String> sliceByTagValues, List<String> groupByTags,
                                Collection<TimeSeries> expected) throws Exception {

    CubeQuery query = new CubeQuery(startTs, endTs, resolution, measureName, MeasureType.COUNTER,
                                    sliceByTagValues, groupByTags);

    Collection<TimeSeries> result = cube.query(query);

    Assert.assertEquals(expected.size(), result.size());
    System.out.println(expected);
    System.out.println(result);
    Assert.assertTrue(expected.containsAll(result));
  }

  private List<TimeValue> timeValues(long... longs) {
    List<TimeValue> timeValues = Lists.newArrayList();
    for (int i = 0; i < longs.length; i += 2) {
      timeValues.add(new TimeValue(longs[i], longs[i + 1]));
    }
    return timeValues;
  }

  private Map<String, String> tagValuesWithNulls(String... tags) {
    Map<String, String> tagValues = Maps.newTreeMap();
    for (int i = 0; i < tags.length; i += 2) {
      tagValues.put(tags[i], tags[i + 1]);
    }
    return tagValues;
  }
}
