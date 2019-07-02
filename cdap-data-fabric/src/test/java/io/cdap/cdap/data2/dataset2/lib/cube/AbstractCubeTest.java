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

package io.cdap.cdap.data2.dataset2.lib.cube;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.cdap.cdap.api.dataset.lib.cube.AggregationFunction;
import io.cdap.cdap.api.dataset.lib.cube.AggregationOption;
import io.cdap.cdap.api.dataset.lib.cube.Cube;
import io.cdap.cdap.api.dataset.lib.cube.CubeDeleteQuery;
import io.cdap.cdap.api.dataset.lib.cube.CubeFact;
import io.cdap.cdap.api.dataset.lib.cube.CubeQuery;
import io.cdap.cdap.api.dataset.lib.cube.Interpolator;
import io.cdap.cdap.api.dataset.lib.cube.Interpolators;
import io.cdap.cdap.api.dataset.lib.cube.MeasureType;
import io.cdap.cdap.api.dataset.lib.cube.TimeSeries;
import io.cdap.cdap.api.dataset.lib.cube.TimeValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

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
      getFact("metric1", 10, 2, MeasureType.COUNTER, "1", "1", "1", "1"),
      getFact("metric1", 11, 3, MeasureType.COUNTER, "1", "1", "1", null),
      getFact("metric1", 12, 4, MeasureType.COUNTER, "2", "1", "1", "1"),
      getFact("metric1", 13, 5, MeasureType.COUNTER, null, null, null, "1")
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
    Map<String, String> deleteTags = new LinkedHashMap<>();
    deleteTags.put("dim1", "1");
    deleteTags.put("dim2", "1");
    deleteTags.put("dim3", "1");

    Predicate<List<String>> predicate =
      aggregates -> Collections.indexOfSubList(aggregates, new ArrayList<>(deleteTags.keySet())) == 0;
    CubeDeleteQuery query = new CubeDeleteQuery(0, 8, resolution, deleteTags,
                                                Collections.singletonList("metric1"), predicate);
    cube.delete(query);

    verifyCountQuery(cube, 0, 15, resolution, "metric1",  AggregationFunction.SUM,
                     ImmutableMap.of("dim1", "1", "dim2", "1", "dim3", "1"),
                     ImmutableList.<String>of(),
                     ImmutableList.of(
                       new TimeSeries("metric1", new HashMap<>(), timeValues(10, 2, 11, 3))));

    // delete cube data for "metric1" for dim1->1 and dim2->1  and check by scanning dim1->1 and dim2->1 is empty,
    deleteTags.remove("dim3");
    query = new CubeDeleteQuery(0, 15, resolution, deleteTags, Collections.singletonList("metric1"),
                                predicate);
    cube.delete(query);

    verifyCountQuery(cube, 0, 15, resolution, "metric1",  AggregationFunction.SUM,
                     ImmutableMap.of("dim1", "1", "dim2", "1"),
                     ImmutableList.<String>of(), ImmutableList.<TimeSeries>of());

  }

  @Test
  public void testIncrements() throws Exception {
    Aggregation agg1 = new DefaultAggregation(ImmutableList.of("dim1"));
    Aggregation agg2 = new DefaultAggregation(ImmutableList.of("dim1", "dim2"));

    int res1 = 1;
    int res100 = 100;
    Cube cube = getCube("myIncCube", new int[] {res1, res100},
                        ImmutableMap.of("agg1", agg1, "agg2", agg2));

    // write some data
    writeInc(cube, "metric1",  1,  1,  "1",  "1");
    writeInc(cube, "metric1",  1,  2,  "2",  "1");
    writeInc(cube, "metric1",  1,  3,  "1",  "2");
    writeInc(cube, "metric2",  1,  4,  "1",  "1");
    writeInc(cube, "metric1",  1,  5,  "1",  "2");
    writeInc(cube, "metric1", 10, 6, "1", "1");
    writeInc(cube, "metric1", 101, 7, "1", "1");

    // now let's query!
    verifyCountQuery(cube, "agg1", 0, 150, res1, "metric1", AggregationFunction.SUM,
                     ImmutableMap.of("dim1", "1"), new ArrayList<String>(),
                     ImmutableList.of(
                       new TimeSeries("metric1", new HashMap<String, String>(), timeValues(1, 9, 10, 6, 101, 7))));

    verifyCountQuery(cube, "agg1", 0, 150, res100, "metric1", AggregationFunction.SUM,
                     ImmutableMap.of("dim1", "1"), new ArrayList<String>(),
                     ImmutableList.of(
                       new TimeSeries("metric1", new HashMap<String, String>(), timeValues(0, 15, 100, 7))));

    verifyCountQuery(cube, "agg2", 0, 150, res1, "metric1",  AggregationFunction.SUM,
                     ImmutableMap.of("dim1", "1", "dim2", "1"), new ArrayList<String>(),
                     ImmutableList.of(
                       new TimeSeries("metric1", new HashMap<String, String>(), timeValues(1, 1, 10, 6, 101, 7))));

    verifyCountQuery(cube, "agg2", 0, 150, res100, "metric1",  AggregationFunction.SUM,
                     ImmutableMap.of("dim1", "1", "dim2", "1"), new ArrayList<String>(),
                     ImmutableList.of(
                       new TimeSeries("metric1", new HashMap<String, String>(), timeValues(0, 7, 100, 7))));

  }

  @Test
  public void testGauges() throws Exception {
    Aggregation agg1 = new DefaultAggregation(ImmutableList.of("dim1"));
    Aggregation agg2 = new DefaultAggregation(ImmutableList.of("dim1", "dim2"));

    int res1 = 1;
    int res100 = 100;
    Cube cube = getCube("myGaugeCube", new int[] {res1, res100},
                        ImmutableMap.of("agg1", agg1, "agg2", agg2));

    // write some data
    writeGauge(cube, "metric1", 1, 1, "1", "1");
    writeGauge(cube, "metric1", 1, 2, "2", "1");
    writeGauge(cube, "metric1", 1, 3, "1", "2");
    writeGauge(cube, "metric2", 1, 4, "1", "1");
    writeGauge(cube, "metric1", 1, 5, "1", "2");
    writeGauge(cube, "metric1", 10, 6, "1", "1");
    writeGauge(cube, "metric1",  101,  7,  "1",  "1");

    // now let's query!
    verifyCountQuery(cube, "agg1", 0, 150, res1, "metric1", AggregationFunction.LATEST,
                     ImmutableMap.of("dim1", "1"), new ArrayList<String>(),
                     ImmutableList.of(
                       new TimeSeries("metric1", new HashMap<String, String>(), timeValues(1, 5, 10, 6, 101, 7))));

    verifyCountQuery(cube, "agg1", 0, 150, res100, "metric1", AggregationFunction.LATEST,
                     ImmutableMap.of("dim1", "1"), new ArrayList<String>(),
                     ImmutableList.of(
                       new TimeSeries("metric1", new HashMap<String, String>(), timeValues(0, 6, 100, 7))));

    verifyCountQuery(cube, "agg2", 0, 150, res1, "metric1",  AggregationFunction.LATEST,
                     ImmutableMap.of("dim1", "1", "dim2", "1"), new ArrayList<String>(),
                     ImmutableList.of(
                       new TimeSeries("metric1", new HashMap<String, String>(), timeValues(1, 1, 10, 6, 101, 7))));

    verifyCountQuery(cube, "agg2", 0, 150, res100, "metric1",  AggregationFunction.LATEST,
                     ImmutableMap.of("dim1", "1", "dim2", "1"), new ArrayList<String>(),
                     ImmutableList.of(
                       new TimeSeries("metric1", new HashMap<String, String>(), timeValues(0, 6, 100, 7))));

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

    Map<String, String> deleteTags = new LinkedHashMap<>();
    deleteTags.put("dim1", "1");
    deleteTags.put("dim2", "1");
    deleteTags.put("dim3", "1");
    Predicate<List<String>> predicate =
      aggregates -> Collections.indexOfSubList(aggregates, new ArrayList<>(deleteTags.keySet())) == 0;

    CubeDeleteQuery query = new CubeDeleteQuery(startTs, endTs, resolution, deleteTags,
                                                Collections.singletonList("metric1"), predicate);
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

    query = new CubeDeleteQuery(startTs, endTs, resolution, deleteTags, Collections.singletonList("metric1"),
                                predicate);
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

  @Test
  public void testMetricDeletion() throws Exception {
    // two aggregation groups with different orders
    Aggregation agg1 = new DefaultAggregation(ImmutableList.of("dim1", "dim2", "dim3"),
                                              ImmutableList.of("dim1"));
    Aggregation agg2 = new DefaultAggregation(ImmutableList.of("dim1", "dim3"),
                                              ImmutableList.of("dim3"));

    int resolution = 1;
    Cube cube = getCube("testDeletion", new int[] {resolution},
                        ImmutableMap.of("agg1", agg1, "agg2", agg2));

    Map<String, String> agg1Dims = new LinkedHashMap<>();
    agg1Dims.put("dim1", "1");
    agg1Dims.put("dim2", "1");
    agg1Dims.put("dim3", "1");

    Map<String, String> agg2Dims = new LinkedHashMap<>();
    agg2Dims.put("dim1", "1");
    agg2Dims.put("dim3", "1");


    // write some data
    writeInc(cube, "metric1",  1,  1,  agg1Dims);
    writeInc(cube, "metric2",  3,  3,  agg2Dims);

    // verify data is there
    verifyCountQuery(cube, 0, 15, resolution, "metric1",  AggregationFunction.SUM,
                     agg1Dims, ImmutableList.of(),
                     ImmutableList.of(
                       new TimeSeries("metric1", new HashMap<>(), timeValues(1, 1))));
    verifyCountQuery(cube, 0, 15, resolution, "metric2",  AggregationFunction.SUM,
                     agg2Dims, ImmutableList.of(),
                     ImmutableList.of(
                       new TimeSeries("metric2", new HashMap<>(), timeValues(3, 3))));

    // delete metrics from agg2
    Predicate<List<String>> predicate =
      aggregates -> Collections.indexOfSubList(aggregates, new ArrayList<>(agg2Dims.keySet())) == 0;
    CubeDeleteQuery query =
      new CubeDeleteQuery(0, 15, resolution, agg2Dims, Collections.emptySet(), predicate);
    cube.delete(query);

    // agg1 data should still be there
    verifyCountQuery(cube, 0, 15, resolution, "metric1",  AggregationFunction.SUM,
                     agg1Dims, ImmutableList.of(),
                     ImmutableList.of(
                       new TimeSeries("metric1", new HashMap<>(), timeValues(1, 1))));
    // agg2 data should get deleted
    verifyCountQuery(cube, 0, 15, resolution, "metric2",  AggregationFunction.SUM,
                     agg2Dims, ImmutableList.of(), ImmutableList.of());

    // delete metrics remain for agg1
    predicate = aggregates -> Collections.indexOfSubList(aggregates, new ArrayList<>(agg1Dims.keySet())) == 0;
    query = new CubeDeleteQuery(0, 15, resolution, agg1Dims, Collections.emptySet(), predicate);
    cube.delete(query);
    verifyCountQuery(cube, 0, 15, resolution, "metric1",  AggregationFunction.SUM,
                     agg1Dims, ImmutableList.of(), ImmutableList.of());
  }

  @Test
  public void testMetricsAggregationOptionSum() throws Exception {
    // two aggregation groups
    Aggregation agg1 = new DefaultAggregation(ImmutableList.of("dim1", "dim2", "dim3"),
                                              ImmutableList.of("dim1"));
    Aggregation agg2 = new DefaultAggregation(ImmutableList.of("dim1", "dim2"),
                                              ImmutableList.of("dim1"));

    int resolution = 1;
    Cube cube = getCube("testAggOptionSum", new int[] {resolution},
                        ImmutableMap.of("agg1", agg1, "agg2", agg2));

    Map<String, String> agg1Dims = new LinkedHashMap<>();
    agg1Dims.put("dim1", "tag1");
    agg1Dims.put("dim2", "tag2");
    agg1Dims.put("dim3", "tag3");

    Map<String, String> agg2Dims = new LinkedHashMap<>();
    agg2Dims.put("dim1", "tag1");
    agg2Dims.put("dim2", "tag4");

    // write 100 data points to agg1
    for (int i = 1; i <= 100; i++) {
      writeInc(cube, "metric1",  i,  1,  agg1Dims);
      writeInc(cube, "metric2",  i,  2,  agg1Dims);
    }

    // write 50 data points to agg2
    for (int i = 1; i <= 50; i++) {
      writeInc(cube, "metric1",  i,  3,  agg2Dims);
    }

    // test limit must be greater than 0
    CubeQuery query = new CubeQuery(null, 0, 200, 1, 0,
                                    ImmutableMap.of("metric1", AggregationFunction.SUM),
                                    agg1Dims, Collections.emptyList(),
                                    AggregationOption.SUM,
                                    null);
    try {
      cube.query(query);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }

    query = new CubeQuery(null, 0, 200, 1, -10,
                                    ImmutableMap.of("metric1", AggregationFunction.SUM),
                                    agg1Dims, Collections.emptyList(),
                                    AggregationOption.SUM,
                                    null);
    try {
      cube.query(query);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }

    // test a limit greater than the number data points, all the data points should be returned
    query = new CubeQuery(null, 0, 200, 1, 200,
                          ImmutableMap.of("metric1", AggregationFunction.SUM,
                                          "metric2", AggregationFunction.SUM),
                          agg1Dims, Collections.emptyList(),
                          AggregationOption.SUM,
                          null);
    List<TimeSeries> result = new ArrayList<>(cube.query(query));
    Assert.assertEquals(2, result.size());
    verifySumAggregation(result.get(0), "metric1", 100, 1, 1, 0, 0);
    verifySumAggregation(result.get(1), "metric2", 100, 2, 1, 0, 0);

    // test aggregation option with sum for metric1 and metric2 for agg1, 5 data points for agg1 should get returned
    // for both metric1 and metric2
    query = new CubeQuery(null, 0, 200, 1, 5,
                                    ImmutableMap.of("metric1", AggregationFunction.SUM,
                                                    "metric2", AggregationFunction.SUM),
                                    agg1Dims, Collections.emptyList(),
                                   AggregationOption.SUM,
                                    null);
    result = new ArrayList<>(cube.query(query));
    Assert.assertEquals(2, result.size());
    // metric1 increment by 1 per second, so sum will be 100/5=20, metric2 increment by 2 per second, so sum will be
    // 200/5=40
    verifySumAggregation(result.get(0), "metric1", 5, 20, 20, 0, 0);
    verifySumAggregation(result.get(1), "metric2", 5, 40, 20, 0, 0);

    // test aggregation option with sum for metric1 with tag name dim1, it should return two time series for agg1 and
    // agg2 for metric1, each with 5 data points
    query = new CubeQuery(null, 0, 200, 1, 5,
                          ImmutableMap.of("metric1", AggregationFunction.SUM),
                          ImmutableMap.of("dim1", "tag1"), ImmutableList.of("dim2"),
                          AggregationOption.SUM,
                          null);
    result = new ArrayList<>(cube.query(query));
    Assert.assertEquals(2, result.size());
    // agg1 gets increment by 1 for 100 seconds, so sum will be 100/5=20, agg2 gets increment by 3 for 50 seconds, so
    // sum will be 3*50/5=30
    verifySumAggregation(result.get(0), "metric1", 5, 30, 10, 0, 0);
    verifySumAggregation(result.get(1), "metric1", 5, 20, 20, 0, 0);

    // test metric1 with count 9, this will have a remainder 100%9=1, so there will be 9 aggregated data points, each
    // with partition size 11
    query = new CubeQuery(null, 0, 200, 1, 9,
                          ImmutableMap.of("metric1", AggregationFunction.SUM),
                          agg1Dims, Collections.emptyList(),
                          AggregationOption.SUM,
                          null);
    result = new ArrayList<>(cube.query(query));
    Assert.assertEquals(1, result.size());

    // the rest data points have sum 11
    verifySumAggregation(result.get(0), "metric1", 9, 11, 11, 1, 1);

    // test metric1 with count 70, this will have a remainder 100%70=30, so the result will have last 70 data points,
    // the first 30 data points will be ignored
    query = new CubeQuery(null, 0, 200, 1, 70,
                          ImmutableMap.of("metric1", AggregationFunction.SUM),
                          agg1Dims, Collections.emptyList(),
                          AggregationOption.SUM,
                          null);

    result = new ArrayList<>(cube.query(query));
    Assert.assertEquals(1, result.size());
    // the rest data points have sum 11
    verifySumAggregation(result.get(0), "metric1", 70, 1, 1, 30, 30);
  }

  @Test
  public void testMetricsAggregationOptionLatest() throws Exception {
    Aggregation agg = new DefaultAggregation(ImmutableList.of("dim1", "dim2", "dim3"),
                                             ImmutableList.of("dim1"));

    int resolution = 1;
    Cube cube = getCube("testAggOptionLatest", new int[] {resolution}, ImmutableMap.of("agg", agg));

    Map<String, String> aggDims = new LinkedHashMap<>();
    aggDims.put("dim1", "tag1");
    aggDims.put("dim2", "tag2");
    aggDims.put("dim3", "tag3");

    // write 100 data points to agg
    for (int i = 1; i <= 100; i++) {
      writeGauge(cube, "metric1",  i,  i,  aggDims);
    }

    // query for latest, should have the latest value for each interval, 20, 40, 60, 80, 100
    CubeQuery query = new CubeQuery(null, 0, 200, 1, 5,
                                    ImmutableMap.of("metric1", AggregationFunction.SUM),
                                    aggDims, Collections.emptyList(),
                                    AggregationOption.LATEST,
                                    null);
    List<TimeSeries> result = new ArrayList<>(cube.query(query));
    Assert.assertEquals(1, result.size());
    List<TimeValue> timeValues = result.get(0).getTimeValues();
    for (int i = 0; i < timeValues.size(); i++) {
      Assert.assertEquals(20 * (i + 1), timeValues.get(i).getValue());
    }
  }

  private void verifySumAggregation(TimeSeries timeSeries, String metricName, int numPoints, int sum,
                                    int timeInterval, int startIndex, int remainder) {
    List<TimeValue> timeValues = timeSeries.getTimeValues();
    Assert.assertEquals(numPoints, timeValues.size());
    Assert.assertEquals(metricName, timeSeries.getMeasureName());
    for (int i = startIndex; i < timeValues.size(); i++) {
      Assert.assertEquals(remainder + timeInterval * (i + 1), timeValues.get(i).getTimestamp());
      Assert.assertEquals(sum, timeValues.get(i).getValue());
    }
  }

  protected void writeInc(Cube cube, String measureName, long ts, long value, String... dims) throws Exception {
    cube.add(getFact(measureName, ts, value, MeasureType.COUNTER, dims));
  }

  protected void writeInc(Cube cube, String mearsureName, long ts, long value,
                          Map<String, String> dims) throws Exception {
    cube.add(getFact(mearsureName, ts, value, MeasureType.COUNTER, dims));
  }

  protected void writeGauge(Cube cube, String measureName, long ts, long value, String... dims) throws Exception {
    cube.add(getFact(measureName, ts, value, MeasureType.GAUGE, dims));
  }

  protected void writeGauge(Cube cube, String mearsureName, long ts, long value,
                            Map<String, String> dims) throws Exception {
    cube.add(getFact(mearsureName, ts, value, MeasureType.GAUGE, dims));
  }

  private void writeIncViaBatchWritable(Cube cube, String measureName, long ts,
                                        long value, String... dims) throws Exception {
    // null for key: it is ignored
    cube.write(null, getFact(measureName, ts, value, MeasureType.COUNTER, dims));
  }

  private CubeFact getFact(String measureName, long ts, long value, MeasureType measureType, String... dims) {
    return getFact(measureName, ts, value, measureType, dimValuesByValues(dims));
  }

  private CubeFact getFact(String measureName, long ts, long value, MeasureType measureType, Map<String, String> dims) {
    return new CubeFact(ts)
      .addDimensionValues(dims)
      .addMeasurement(measureName, measureType, value);
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
    Assert.assertEquals(String.format("expected: %s, found: %s", expected, result), expected.size(), result.size());
    Assert.assertTrue(String.format("expected: %s, found: %s", expected, result), expected.containsAll(result));
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
