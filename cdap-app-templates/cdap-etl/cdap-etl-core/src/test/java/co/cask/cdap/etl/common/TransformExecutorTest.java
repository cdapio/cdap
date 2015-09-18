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
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 */
public class TransformExecutorTest {

  @Test
  public void testEmptyTransforms() throws Exception {
    TransformExecutor<String, String> executor =
      new TransformExecutor<>(Lists.<TransformDetail>newArrayList());
    TransformResponse transformResponse = executor.runOneIteration("foo");
    List<String> results = Lists.newArrayList(transformResponse.getEmittedRecords());
    Assert.assertEquals(1, results.size());
    Assert.assertEquals("foo", results.get(0));
    executor.resetEmitters();
  }

  @Test
  public void testTransforms() throws Exception {
    MockMetrics mockMetrics = new MockMetrics();
    List<TransformDetail> transforms = Lists.<TransformDetail>newArrayList();
    transforms.add(
      new TransformDetail("intToDoubleTransform", new IntToDouble(),
                                new StageMetrics(mockMetrics, PluginID.from(Constants.Source.PLUGINTYPE, "first", 1))));

    transforms.add(
      new TransformDetail("filterTransform", new Filter(100d),
                                new StageMetrics(mockMetrics,
                                                 PluginID.from(Constants.Transform.PLUGINTYPE, "second", 2))));

    transforms.add(
      new TransformDetail("doubleToStringTransform", new DoubleToString(),
                                new StageMetrics(mockMetrics, PluginID.from(Constants.Sink.PLUGINTYPE, "third", 3))));

    TransformExecutor<Integer, String> executor = new TransformExecutor<>(transforms);

    TransformResponse transformResponse = executor.runOneIteration(1);
    List<String> results = Lists.newArrayList(transformResponse.getEmittedRecords());
    Assert.assertTrue(results.isEmpty());
    Map<String, Collection> errorIteratorsMap = transformResponse.getMapTransformIdToErrorEmitter();

    Assert.assertEquals(1, errorIteratorsMap.size());
    Assert.assertEquals(3, Lists.newArrayList(errorIteratorsMap.get("filterTransform")).size());

    Assert.assertEquals(3, mockMetrics.getCount("source.first.1.records.out"));
    Assert.assertEquals(0, mockMetrics.getCount("transform.second.2.records.out"));
    Assert.assertEquals(0, mockMetrics.getCount("sink.third.3.records.out"));

    executor.resetEmitters();
    results = Lists.newArrayList(executor.runOneIteration(10).getEmittedRecords());
    Assert.assertEquals(1, results.size());
    Assert.assertEquals("1000.0", results.get(0));
    Assert.assertEquals(6, mockMetrics.getCount("source.first.1.records.out"));
    Assert.assertEquals(1, mockMetrics.getCount("transform.second.2.records.out"));
    Assert.assertEquals(1, mockMetrics.getCount("sink.third.3.records.out"));

    executor.resetEmitters();
    results = Lists.newArrayList(executor.runOneIteration(100).getEmittedRecords());
    Assert.assertEquals(2, results.size());
    Assert.assertEquals("1000.0", results.get(0));
    Assert.assertEquals("10000.0", results.get(1));
    Assert.assertEquals(9, mockMetrics.getCount("source.first.1.records.out"));
    Assert.assertEquals(3, mockMetrics.getCount("transform.second.2.records.out"));
    Assert.assertEquals(3, mockMetrics.getCount("sink.third.3.records.out"));

    executor.resetEmitters();
  }

  private static class IntToDouble extends Transform<Integer, Double> {
    @Override
    public void transform(Integer input, Emitter<Double> emitter) throws Exception {
      emitter.emit(input.doubleValue());
      emitter.emit(10 * input.doubleValue());
      emitter.emit(100 * input.doubleValue());
    }
  }

  private static class Filter extends Transform<Double, Double> {
    private final Double threshold;

    public Filter(Double threshold) {
      this.threshold = threshold;
    }

    @Override
    public void transform(Double input, Emitter<Double> emitter) throws Exception {
      if (input > threshold) {
        emitter.emit(input);
      } else {
        emitter.emitError(new InvalidEntry<Double>(100, "less than threshold", input));
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
