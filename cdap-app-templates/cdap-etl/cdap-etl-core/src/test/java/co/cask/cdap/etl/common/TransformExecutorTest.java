/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.common;

import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.Transformation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class TransformExecutorTest {

  @Test
  public void testEmptyTransforms() throws Exception {
    MockMetrics mockMetrics = new MockMetrics();
    Map<String, Transformation> transformationMap = new HashMap<>();
    transformationMap.put("sink", new DoubleToString());
    TransformDetail transformDetail = new TransformDetail(transformationMap, mockMetrics);
    Map<String, List<String>> connectionsMap = new HashMap<>();
    connectionsMap.put("source", ImmutableList.of("sink"));

    TransformExecutor executor =
      new TransformExecutor(transformDetail, connectionsMap, "source");
    TransformResponse transformResponse = executor.runOneIteration(1d);
    Map<String, List<Object>> sinkResult = transformResponse.getSinksResults();
    Assert.assertTrue(sinkResult.containsKey("sink"));
    List<Object> sinkResultList = sinkResult.get("sink");
    Assert.assertEquals(1, sinkResultList.size());
    // note : sink transform would have exectued, so the expected is string and not integer
    Assert.assertEquals("1.0", sinkResultList.get(0));
    executor.resetEmitter();
  }


  @Test
  public void testTransforms() throws Exception {
    MockMetrics mockMetrics = new MockMetrics();
    Map<String, Transformation> transformationMap = new HashMap<>();

    transformationMap.put("transform1", new IntToDouble());
    transformationMap.put("transform2", new Filter(100d, Threshold.LOWER));
    transformationMap.put("sink1", new DoubleToString());
    transformationMap.put("sink2", new DoubleToString());

    TransformDetail transformDetail = new TransformDetail(transformationMap, mockMetrics);

    Map<String, List<String>> connectionsMap = new HashMap<>();

    connectionsMap.put("source", ImmutableList.of("transform1"));
    connectionsMap.put("transform1", ImmutableList.of("transform2", "sink1"));
    connectionsMap.put("transform2", ImmutableList.of("sink2"));

    TransformExecutor<Integer> executor = new TransformExecutor<>(transformDetail, connectionsMap, "source");

    TransformResponse transformResponse = executor.runOneIteration(1);

    assertResults(transformResponse.getSinksResults(), ImmutableMap.of("sink1", 3));

    assertResults(transformResponse.getMapTransformIdToErrorEmitter(), ImmutableMap.of("transform2", 3));

    Assert.assertEquals(3, mockMetrics.getCount("transform1.records.out"));
    Assert.assertEquals(0, mockMetrics.getCount("transform2.records.out"));
    Assert.assertEquals(3, mockMetrics.getCount("sink1.records.out"));
    Assert.assertEquals(0, mockMetrics.getCount("sink2.records.out"));
    executor.resetEmitter();
    mockMetrics.clearMetrics();


    transformResponse = executor.runOneIteration(10);

    assertResults(transformResponse.getSinksResults(), ImmutableMap.of("sink1", 3, "sink2", 1));

    assertResults(transformResponse.getMapTransformIdToErrorEmitter(), ImmutableMap.of("transform2", 2));

    Assert.assertEquals(3, mockMetrics.getCount("transform1.records.out"));
    Assert.assertEquals(1, mockMetrics.getCount("transform2.records.out"));
    Assert.assertEquals(3, mockMetrics.getCount("sink1.records.out"));
    Assert.assertEquals(1, mockMetrics.getCount("sink2.records.out"));
    executor.resetEmitter();
    mockMetrics.clearMetrics();

    transformResponse = executor.runOneIteration(100);

    assertResults(transformResponse.getSinksResults(), ImmutableMap.of("sink1", 3, "sink2", 2));

    assertResults(transformResponse.getMapTransformIdToErrorEmitter(), ImmutableMap.of("transform2", 1));
    Assert.assertEquals(3, mockMetrics.getCount("transform1.records.out"));
    Assert.assertEquals(2, mockMetrics.getCount("transform2.records.out"));
    Assert.assertEquals(3, mockMetrics.getCount("sink1.records.out"));
    Assert.assertEquals(2, mockMetrics.getCount("sink2.records.out"));
    executor.resetEmitter();
    mockMetrics.clearMetrics();

    transformResponse = executor.runOneIteration(2000);

    assertResults(transformResponse.getSinksResults(), ImmutableMap.of("sink1", 3, "sink2", 3));

    assertResults(transformResponse.getMapTransformIdToErrorEmitter(), new HashMap<String, Integer>());
    Assert.assertEquals(3, mockMetrics.getCount("transform1.records.out"));
    Assert.assertEquals(3, mockMetrics.getCount("transform2.records.out"));
    Assert.assertEquals(3, mockMetrics.getCount("sink1.records.out"));
    Assert.assertEquals(3, mockMetrics.getCount("sink2.records.out"));
    executor.resetEmitter();
    mockMetrics.clearMetrics();
  }

  @Test
  public void testTransformsWithMerge() throws Exception {
    MockMetrics mockMetrics = new MockMetrics();
    Map<String, Transformation> transformationMap = new HashMap<>();

    transformationMap.put("conversion", new IntToDouble());
    transformationMap.put("filter1", new Filter(100d, Threshold.LOWER));
    transformationMap.put("filter2", new Filter(1000d, Threshold.LOWER));
    transformationMap.put("limiter1", new Filter(5000d, Threshold.UPPER));
    transformationMap.put("sink1", new DoubleToString());
    transformationMap.put("sink2", new DoubleToString());
    transformationMap.put("sink3", new DoubleToString());

    TransformDetail transformDetail = new TransformDetail(transformationMap, mockMetrics);

    Map<String, List<String>> connectionsMap = new HashMap<>();

    connectionsMap.put("source", ImmutableList.of("conversion"));
    connectionsMap.put("conversion", ImmutableList.of("filter1", "filter2"));
    connectionsMap.put("filter1", ImmutableList.of("limiter1", "sink1"));
    connectionsMap.put("filter2", ImmutableList.of("limiter1", "sink2"));
    connectionsMap.put("limiter1", ImmutableList.of("sink3"));

    TransformExecutor<Integer> executor = new TransformExecutor<>(transformDetail, connectionsMap, "source");
    TransformResponse transformResponse = executor.runOneIteration(200);
    assertResults(transformResponse.getSinksResults(), ImmutableMap.of("sink1", 3, "sink2", 2, "sink3", 3));
    assertResults(transformResponse.getMapTransformIdToErrorEmitter(), ImmutableMap.of("filter2", 1, "limiter1", 2));
    Assert.assertEquals(3, mockMetrics.getCount("filter1.records.in"));
    Assert.assertEquals(3, mockMetrics.getCount("filter1.records.out"));

    Assert.assertEquals(3, mockMetrics.getCount("filter2.records.in"));
    Assert.assertEquals(2, mockMetrics.getCount("filter2.records.out"));

    Assert.assertEquals(5, mockMetrics.getCount("limiter1.records.in"));
    Assert.assertEquals(3, mockMetrics.getCount("limiter1.records.out"));

    Assert.assertEquals(3, mockMetrics.getCount("sink1.records.out"));
    Assert.assertEquals(2, mockMetrics.getCount("sink2.records.out"));
    Assert.assertEquals(3, mockMetrics.getCount("sink3.records.out"));
  }

  private <T> void assertResults(Map<String, List<T>> results, Map<String, Integer> expectedListsSize) {
    Assert.assertEquals(expectedListsSize.size(), results.size());
    for (Map.Entry<String, Integer> entry : expectedListsSize.entrySet()) {
      Assert.assertTrue(results.containsKey(entry.getKey()));
      Assert.assertEquals(entry.getValue().intValue(), results.get(entry.getKey()).size());
    }
  }



  private static class IntToDouble extends Transform<Integer, Double> {

    @Override
    public void transform(Integer input, Emitter<Double> emitter) throws Exception {
      emitter.emit(input.doubleValue());
      emitter.emit(10 * input.doubleValue());
      emitter.emit(100 * input.doubleValue());
    }
  }

  private static enum Threshold {
    LOWER,
    UPPER
  }

  private static class Filter extends Transform<Double, Double> {
    private final Double threshold;
    private final Threshold thresholdType;

    public Filter(Double threshold, Threshold thresholdType) {
      this.threshold = threshold;
      this.thresholdType = thresholdType;
    }

    @Override
    public void transform(Double input, Emitter<Double> emitter) throws Exception {
      if (thresholdType.equals(Threshold.LOWER)) {
        if (input > threshold) {
          emitter.emit(input);
        } else {
          emitter.emitError(new InvalidEntry<>(100, "less than threshold ", input));
        }
      } else {
        if (input < threshold) {
          emitter.emit(input);
        } else {
          emitter.emitError(new InvalidEntry<>(200, "greater than limit ", input));
        }
      }
    }
  }

  private static class DoubleToString extends Transform<Double, String> {

    @Override
    public void transform(Double input, Emitter<String> emitter) throws Exception {
      emitter.emit(String.valueOf(input));
    }
  }
}
