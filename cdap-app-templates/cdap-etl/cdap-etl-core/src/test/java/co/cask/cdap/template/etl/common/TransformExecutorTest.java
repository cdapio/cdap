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

package co.cask.cdap.template.etl.common;

import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.Transform;
import co.cask.cdap.template.etl.api.Transformation;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 */
public class TransformExecutorTest {

  @Test
  public void testEmptyTransforms() throws Exception {
    TransformExecutor<String, String> executor =
      new TransformExecutor<String, String>(Lists.<Transformation>newArrayList(), Lists.<StageMetrics>newArrayList());
    List<String> results = Lists.newArrayList(executor.runOneIteration("foo"));
    Assert.assertEquals(1, results.size());
    Assert.assertEquals("foo", results.get(0));
  }

  @Test
  public void testTransforms() throws Exception {
    MockMetrics mockMetrics = new MockMetrics();
    List<Transformation> transforms = Lists.<Transformation>newArrayList(
      new IntToDouble(), new Filter(100d), new DoubleToString());
    List<StageMetrics> stageMetrics = Lists.newArrayList(
      new StageMetrics(mockMetrics, StageMetrics.Type.SOURCE, "first"),
      new StageMetrics(mockMetrics, StageMetrics.Type.TRANSFORM, "second"),
      new StageMetrics(mockMetrics, StageMetrics.Type.SINK, "third")
    );
    TransformExecutor<Integer, String> executor = new TransformExecutor<Integer, String>(transforms, stageMetrics);

    List<String> results = Lists.newArrayList(executor.runOneIteration(1));
    Assert.assertTrue(results.isEmpty());
    Assert.assertEquals(3, mockMetrics.getCount("source.first.records.out"));
    Assert.assertEquals(0, mockMetrics.getCount("transform.second.records.out"));
    Assert.assertEquals(0, mockMetrics.getCount("sink.third.records.out"));

    results = Lists.newArrayList(executor.runOneIteration(10));
    Assert.assertEquals(1, results.size());
    Assert.assertEquals("1000.0", results.get(0));
    Assert.assertEquals(6, mockMetrics.getCount("source.first.records.out"));
    Assert.assertEquals(1, mockMetrics.getCount("transform.second.records.out"));
    Assert.assertEquals(1, mockMetrics.getCount("sink.third.records.out"));

    results = Lists.newArrayList(executor.runOneIteration(100));
    Assert.assertEquals(2, results.size());
    Assert.assertEquals("1000.0", results.get(0));
    Assert.assertEquals("10000.0", results.get(1));
    Assert.assertEquals(9, mockMetrics.getCount("source.first.records.out"));
    Assert.assertEquals(3, mockMetrics.getCount("transform.second.records.out"));
    Assert.assertEquals(3, mockMetrics.getCount("sink.third.records.out"));
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
