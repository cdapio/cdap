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

package co.cask.cdap.data2.dataset2.lib.cube;

import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.dataset.lib.cube.CubeDeleteQuery;
import co.cask.cdap.api.dataset.lib.cube.CubeFact;
import co.cask.cdap.api.dataset.lib.cube.CubeQuery;
import co.cask.cdap.api.dataset.lib.cube.Interpolator;
import co.cask.cdap.api.dataset.lib.cube.Interpolators;
import co.cask.cdap.api.dataset.lib.cube.MeasureType;
import co.cask.cdap.api.dataset.lib.cube.TimeSeries;
import co.cask.cdap.api.dataset.lib.cube.TimeValue;
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
import java.util.concurrent.TimeUnit;

/**
 *
 */
public abstract class AbstractCubeTest {
  protected abstract Cube getCube(String name, int[] resolutions,
                                  Map<String, ? extends Aggregation> aggregations) throws Exception;

  @Test
  public void testBasics() throws Exception {
    Aggregation agg1 = new DefaultAggregation(ImmutableList.of("dim1", "dim2", "dim3"),
                                              ImmutableList.of("dim1", "dim2"));
    Aggregation agg2 = new DefaultAggregation(ImmutableList.of("dim1", "dim2"),
                                              ImmutableList.of("dim1"));

    int resolution = 1;
    Cube cube = getCube("myCube", new int[] {resolution},
                        ImmutableMap.of("agg1", agg1, "agg2", agg2));

    // write some data
    // NOTE: we mostly use different ts, as we are interested in checking incs not at persist, but rather at query time
    writeInc(cube, "metric1",  1,  1,  "1",  "1",  "1");
    writeInc(cube, "metric1",  1,  1,  "1",  "1",  "1");
    writeInc(cube, "metric1",  2,  2, null,  "1",  "1");
    writeInc(cube, "metric1",  3,  3,  "1",  "2",  "1");
    writeInc(cube, "metric1",  3,  5,  "1",  "2",  "3");
    writeInc(cube, "metric1",  3,  7,  "2",  "1",  "1");
    writeInc(cube, "metric1", 4, 4, "1", null, "2");
    writeInc(cube, "metric1", 5, 5, null, null, "1");
    writeInc(cube, "metric1", 6, 6, "1", null, null);
    writeInc(cube, "metric1",  7,  3,  "1",  "1", null);
    // writing using BatchWritable APIs
    writeIncViaBatchWritable(cube, "metric1", 8, 2, null, "1", null);
    writeIncViaBatchWritable(cube, "metric1", 9, 1, null, null, null);
    // writing in batch
    cube.add(ImmutableList.of(
      getFact("metric1", 10, 2, "1", "1", "1", "1"),
      getFact("metric1", 11, 3, "1", "1", "1", null),
      getFact("metric1", 12, 4, "2", "1", "1", "1"),
      getFact("metric1", 13, 5, null, null, null, "1")
    ));

    writeInc(cube, "metric2", 1, 1, "1", "1", "1");

    // todo: do some write instead of increments - test those as well

    // now let's query!
    verifyCountQuery(cube, 0, 15, resolution, "metric1",  AggregationFunction.SUM,
                     ImmutableMap.of("dim1", "1"), ImmutableList.of("dim2"),
                     ImmutableList.of(
                       new TimeSeries("metric1", dimensionValues("dim2", "1"), timeValues(1, 2, 7, 3, 10, 2, 11, 3)),
                       new TimeSeries("metric1", dimensionValues("dim2", "2"), timeValues(3, 8))));

    verifyCountQuery(cube, 0, 15, resolution, "metric1",  AggregationFunction.SUM,
                     ImmutableMap.of("dim1", "1", "dim2", "1", "dim3", "1"),
                     new ArrayList<String>(),
                     ImmutableList.of(
                       new TimeSeries("metric1", new HashMap<String, String>(), timeValues(1, 2, 10, 2, 11, 3))));

    verifyCountQuery(cube, 0, 15, resolution, "metric1",  AggregationFunction.SUM,
                     new HashMap<String, String>(), ImmutableList.of("dim1"),
                     ImmutableList.of(
                       new TimeSeries("metric1", dimensionValues("dim1", "1"),
                                      timeValues(1, 2, 3, 8, 4, 4, 6, 6, 7, 3, 10, 2, 11, 3)),
                       new TimeSeries("metric1", dimensionValues("dim1", "2"),
                                      timeValues(3, 7, 12, 4))));

    verifyCountQuery(cube, 0, 15, resolution, "metric1",  AggregationFunction.SUM,
                     ImmutableMap.of("dim3", "3"), new ArrayList<String>(),
                     ImmutableList.of(
                       new TimeSeries("metric1", new HashMap<String, String>(), timeValues(3, 5))));


    // test querying specific aggregations
    verifyCountQuery(cube, "agg1", 0, 15, resolution, "metric1",  AggregationFunction.SUM,
                     ImmutableMap.of("dim1", "1"), new ArrayList<String>(),
                     ImmutableList.of(new TimeSeries("metric1", new HashMap<String, String>(),
                                                     timeValues(1, 2, 3, 8, 7, 3, 10, 2, 11, 3))));
    verifyCountQuery(cube, "agg2", 0, 15, resolution, "metric1",  AggregationFunction.SUM,
                     ImmutableMap.of("dim1", "1"), new ArrayList<String>(),
                     ImmutableList.of(new TimeSeries("metric1", new HashMap<String, String>(),
                                                     timeValues(1, 2, 3, 8, 4, 4, 6, 6, 7, 3, 10, 2, 11, 3))));

    // query with different agg functions
    verifyCountQuery(cube, "agg1", 0, 15, resolution, "metric1",  AggregationFunction.MAX,
                     ImmutableMap.of("dim1", "1"), new ArrayList<String>(),
                     ImmutableList.of(new TimeSeries("metric1", new HashMap<String, String>(),
                                                     timeValues(1, 2, 3, 5, 7, 3, 10, 2, 11, 3))));
    verifyCountQuery(cube, "agg1", 0, 15, resolution, "metric1",  AggregationFunction.MIN,
                     ImmutableMap.of("dim1", "1"), new ArrayList<String>(),
                     ImmutableList.of(new TimeSeries("metric1", new HashMap<String, String>(),
                                                     timeValues(1, 2, 3, 3, 7, 3, 10, 2, 11, 3))));
    verifyCountQuery(cube, "agg1", 0, 15, resolution, "metric1",  AggregationFunction.LATEST,
                     ImmutableMap.of("dim1", "1"), new ArrayList<String>(),
                     ImmutableList.of(new TimeSeries("metric1", new HashMap<String, String>(),
                                                     timeValues(1, 2, 3, 5, 7, 3, 10, 2, 11, 3))));


    // delete cube data for "metric1" for dim->1,dim2->1,dim3->1 for timestamp 1 - 8 and
    // check data for other timestamp is available

    CubeDeleteQuery query = new CubeDeleteQuery(0, 8, resolution,
                                                ImmutableMap.of("dim1", "1", "dim2", "1", "dim3", "1"),
                                                "metric1");
    cube.delete(query);

    verifyCountQuery(cube, 0, 15, resolution, "metric1",  AggregationFunction.SUM,
                     ImmutableMap.of("dim1", "1", "dim2", "1", "dim3", "1"),
                     ImmutableList.<String>of(),
                     ImmutableList.of(
                       new TimeSeries("metric1", new HashMap<String, String>(), timeValues(10, 2, 11, 3))));

    // delete cube data for "metric1" for dim1->1 and dim2->1  and check by scanning dim1->1 and dim2->1 is empty,

    query = new CubeDeleteQuery(0, 15, resolution, ImmutableMap.of("dim1", "1", "dim2", "1"),
                                "metric1");
    cube.delete(query);

    verifyCountQuery(cube, 0, 15, resolution, "metric1",  AggregationFunction.SUM,
                     ImmutableMap.of("dim1", "1", "dim2", "1"),
                     ImmutableList.<String>of(), ImmutableList.<TimeSeries>of());

  }

  @Test
  public void testInterpolate() throws Exception {
    Aggregation agg1 = new DefaultAggregation(ImmutableList.of("dim1", "dim2", "dim3"),
                                              ImmutableList.of("dim1", "dim2", "dim3"));
    int resolution = 1;
    Cube cube = getCube("myInterpolatedCube", new int[] {resolution},
                        ImmutableMap.of("agg1", agg1));
    // test step interpolation
    long startTs = 1;
    long endTs = 10;
    writeInc(cube, "metric1",  startTs,  5,  "1",  "1",  "1");
    writeInc(cube, "metric1",  endTs,  3,  "1",  "1",  "1");
    List<TimeValue> expectedTimeValues = Lists.newArrayList();
    for (long i = startTs; i < endTs; i++) {
      expectedTimeValues.add(new TimeValue(i, 5));
    }
    expectedTimeValues.add(new TimeValue(endTs, 3));
    verifyCountQuery(cube, startTs, endTs, resolution, "metric1", AggregationFunction.SUM,
                     ImmutableMap.of("dim1", "1", "dim2", "1", "dim3", "1"),
                     new ArrayList<String>(),
                     ImmutableList.of(
                       new TimeSeries("metric1", new HashMap<String, String>(), expectedTimeValues)),
                     new Interpolators.Step());

    CubeDeleteQuery query = new CubeDeleteQuery(startTs, endTs, resolution,
                                                ImmutableMap.of("dim1", "1", "dim2", "1", "dim3", "1"),
                                                "metric1");
    cube.delete(query);
    //test small-slope linear interpolation
    startTs = 1;
    endTs = 5;
    writeInc(cube, "metric1",  startTs,  5,  "1",  "1",  "1");
    writeInc(cube, "metric1",  endTs,  3,  "1",  "1",  "1");
    verifyCountQuery(cube, startTs, endTs, resolution, "metric1", AggregationFunction.SUM,
                     ImmutableMap.of("dim1", "1", "dim2", "1", "dim3", "1"),
                     new ArrayList<String>(),
                     ImmutableList.of(
                       new TimeSeries("metric1", new HashMap<String, String>(), timeValues(1, 5, 2, 5, 3, 4,
                                                                                           4, 4, 5, 3))),
                     new Interpolators.Linear());

    query = new CubeDeleteQuery(startTs, endTs, resolution,
                                ImmutableMap.of("dim1", "1", "dim2", "1", "dim3", "1"),
                                "metric1");
    cube.delete(query);

    //test big-slope linear interpolation
    writeInc(cube, "metric1",  startTs,  100,  "1",  "1",  "1");
    writeInc(cube, "metric1",  endTs,  500,  "1",  "1",  "1");
    verifyCountQuery(cube, startTs, endTs, resolution, "metric1", AggregationFunction.SUM,
                     ImmutableMap.of("dim1", "1", "dim2", "1", "dim3", "1"),
                     new ArrayList<String>(),
                     ImmutableList.of(
                       new TimeSeries("metric1", new HashMap<String, String>(), timeValues(1, 100, 2, 200, 3, 300,
                                                                                           4, 400, 5, 500))),
                     new Interpolators.Linear());

    cube.delete(query);

    //test limit on Interpolate
    long limit = 20;
    writeInc(cube, "metric1",  0,  10,  "1",  "1",  "1");
    writeInc(cube, "metric1",  limit + 1,  50,  "1",  "1",  "1");
    expectedTimeValues.clear();
    expectedTimeValues.add(new TimeValue(0, 10));
    for (long i = 1; i <= limit; i++) {
      expectedTimeValues.add(new TimeValue(i, 0));
    }
    expectedTimeValues.add(new TimeValue(limit + 1, 50));
    verifyCountQuery(cube, 0, 21, resolution, "metric1", AggregationFunction.SUM,
                     ImmutableMap.of("dim1", "1", "dim2", "1", "dim3", "1"),
                     new ArrayList<String>(),
                     ImmutableList.of(
                       new TimeSeries("metric1", new HashMap<String, String>(), expectedTimeValues)),
                     new Interpolators.Step(limit));

  }


  protected void writeInc(Cube cube, String measureName, long ts, long value, String... dims) throws Exception {
    cube.add(getFact(measureName, ts, value, dims));
  }

  private void writeIncViaBatchWritable(Cube cube, String measureName, long ts,
                                        long value, String... dims) throws Exception {
    // null for key: it is ignored
    cube.write(null, getFact(measureName, ts, value, dims));
  }

  private CubeFact getFact(String measureName, long ts, long value, String... dims) {
    return new CubeFact(ts)
      .addDimensionValues(dimValuesByValues(dims))
      .addMeasurement(measureName, MeasureType.COUNTER, value);
  }

  private Map<String, String> dimValuesByValues(String... dims) {
    Map<String, String> dimValues = Maps.newHashMap();
    for (int i = 0; i < dims.length; i++) {
      if (dims[i] != null) {
        // +1 to start with "dim1"
        dimValues.put("dim" + (i + 1), dims[i]);
      }
    }
    return dimValues;
  }

  private void verifyCountQuery(Cube cube, String aggregation, long startTs, long endTs, int resolution,
                                String measureName, AggregationFunction aggFunction,
                                Map<String, String> dimValues, List<String> groupByDims,
                                Collection<TimeSeries> expected) throws Exception {

    verifyCountQuery(cube, aggregation, startTs, endTs, resolution, measureName, aggFunction,
                     dimValues, groupByDims, expected, null);
  }

  protected void verifyCountQuery(Cube cube, long startTs, long endTs, int resolution,
                                String measureName, AggregationFunction aggFunction,
                                Map<String, String> dimValues, List<String> groupByDims,
                                Collection<TimeSeries> expected) throws Exception {

    verifyCountQuery(cube, null, startTs, endTs, resolution, measureName, aggFunction,
                     dimValues, groupByDims, expected, null);
  }

  private void verifyCountQuery(Cube cube, long startTs, long endTs, int resolution,
                                String measureName, AggregationFunction aggFunction,
                                Map<String, String> dimValues, List<String> groupByDims,
                                Collection<TimeSeries> expected, Interpolator interpolator) throws Exception {

    verifyCountQuery(cube, null, startTs, endTs, resolution, measureName, aggFunction,
                     dimValues, groupByDims, expected, interpolator);
  }

  private void verifyCountQuery(Cube cube, String aggregation, long startTs, long endTs, int resolution,
                                String measureName, AggregationFunction aggFunction,
                                Map<String, String> dimValues, List<String> groupByDims,
                                Collection<TimeSeries> expected, Interpolator interpolator) throws Exception {

    CubeQuery query = CubeQuery.builder()
      .select()
        .measurement(measureName, aggFunction)
      .from(aggregation).resolution(resolution, TimeUnit.SECONDS)
      .where()
        .dimensions(dimValues)
        .timeRange(startTs, endTs)
      .groupBy()
        .dimensions(groupByDims)
      .limit(Integer.MAX_VALUE)
      .interpolator(interpolator)
      .build();

    Collection<TimeSeries> result = cube.query(query);
    Assert.assertEquals(expected.size(), result.size());
    Assert.assertTrue(expected.containsAll(result));
  }

  protected List<TimeValue> timeValues(long... longs) {
    List<TimeValue> timeValues = Lists.newArrayList();
    for (int i = 0; i < longs.length; i += 2) {
      timeValues.add(new TimeValue(longs[i], longs[i + 1]));
    }
    return timeValues;
  }

  private Map<String, String> dimensionValues(String... dims) {
    Map<String, String> dimValues = Maps.newTreeMap();
    for (int i = 0; i < dims.length; i += 2) {
      dimValues.put(dims[i], dims[i + 1]);
    }
    return dimValues;
  }
}
