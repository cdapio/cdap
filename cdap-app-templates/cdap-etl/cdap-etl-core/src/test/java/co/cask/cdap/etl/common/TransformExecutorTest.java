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
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class TransformExecutorTest {
  // TODO : Add more tests

  @Test
  public void testEmptyTransforms() throws Exception {
    TransformDetail transformDetail = new TransformDetail("sink", new DoubleToString(),
                                                          new DefaultStageMetrics(new MockMetrics(), "sink1"), true);

    Map<String, List<TransformDetail>> emptyTransforms = new HashMap<>();
    emptyTransforms.put("source", ImmutableList.of(transformDetail));
    TransformExecutor executor =
      new TransformExecutor(emptyTransforms, "source");
    TransformResponse transformResponse = executor.runOneIteration(1);
    Iterator sinkIterator = transformResponse.getSinksResults().get("sink").iterator();
    Assert.assertTrue(sinkIterator.hasNext());
    // note : sink transform would not be exectued, so the expected is integer and not string
    Assert.assertEquals(1, sinkIterator.next());
    executor.resetEmitters();
  }


  @Test
  public void testTransforms() throws Exception {
    MockMetrics mockMetrics = new MockMetrics();
    Map<String, List<TransformDetail>> transforms = new HashMap<>();

    TransformDetail intToDoubleTransform =
      new TransformDetail("transform1", new IntToDouble(), new DefaultStageMetrics(mockMetrics, "transform1"), false);

    TransformDetail filterTransform =
      new TransformDetail("transform2", new Filter(100d), new DefaultStageMetrics(mockMetrics, "transform2"), false);

    TransformDetail doubleToStringTransform1 =
      new TransformDetail("sink1", new DoubleToString(), new DefaultStageMetrics(mockMetrics, "sink1"), true);

    TransformDetail doubleToStringTransform2 =
      new TransformDetail("sink2", new DoubleToString(), new DefaultStageMetrics(mockMetrics, "sink2"), true);

    transforms.put("source", ImmutableList.of(intToDoubleTransform));
    transforms.put("transform1", ImmutableList.of(filterTransform, doubleToStringTransform1));
    transforms.put("transform2", ImmutableList.of(doubleToStringTransform2));

    TransformExecutor<Integer, String> executor = new TransformExecutor<>(transforms, "source");

    TransformResponse transformResponse = executor.runOneIteration(1);

    assertResults(transformResponse.getSinksResults(), 3, 0);
    assertErrors(transformResponse.getMapTransformIdToErrorEmitter(), true,  3);
    Assert.assertEquals(3, mockMetrics.getCount("transform1.records.out"));
    Assert.assertEquals(0, mockMetrics.getCount("transform2.records.out"));
    Assert.assertEquals(3, mockMetrics.getCount("sink1.records.out"));
    Assert.assertEquals(0, mockMetrics.getCount("sink2.records.out"));
    executor.resetEmitters();
    mockMetrics.clearMetrics();


    transformResponse = executor.runOneIteration(10);

    assertResults(transformResponse.getSinksResults(), 3, 1);
    assertErrors(transformResponse.getMapTransformIdToErrorEmitter(), true,  2);
    Assert.assertEquals(3, mockMetrics.getCount("transform1.records.out"));
    Assert.assertEquals(1, mockMetrics.getCount("transform2.records.out"));
    Assert.assertEquals(3, mockMetrics.getCount("sink1.records.out"));
    Assert.assertEquals(1, mockMetrics.getCount("sink2.records.out"));
    executor.resetEmitters();
    mockMetrics.clearMetrics();

    transformResponse = executor.runOneIteration(100);

    assertResults(transformResponse.getSinksResults(), 3, 2);
    assertErrors(transformResponse.getMapTransformIdToErrorEmitter(), true,  1);
    Assert.assertEquals(3, mockMetrics.getCount("transform1.records.out"));
    Assert.assertEquals(2, mockMetrics.getCount("transform2.records.out"));
    Assert.assertEquals(3, mockMetrics.getCount("sink1.records.out"));
    Assert.assertEquals(2, mockMetrics.getCount("sink2.records.out"));
    executor.resetEmitters();
    mockMetrics.clearMetrics();

    executor.runOneIteration(10);
    assertErrors(transformResponse.getMapTransformIdToErrorEmitter(), true,  2);
    transformResponse = executor.runOneIteration(2000);

    assertResults(transformResponse.getSinksResults(), 6, 4);
    assertErrors(transformResponse.getMapTransformIdToErrorEmitter(), false,  0);
    Assert.assertEquals(6, mockMetrics.getCount("transform1.records.out"));
    Assert.assertEquals(4, mockMetrics.getCount("transform2.records.out"));
    Assert.assertEquals(6, mockMetrics.getCount("sink1.records.out"));
    Assert.assertEquals(4, mockMetrics.getCount("sink2.records.out"));
    executor.resetEmitters();
    mockMetrics.clearMetrics();
  }

  private void assertErrors(Map<String, Collection> mapTransformIdToErrorEmitter, boolean contains, int expected) {
    Assert.assertEquals(contains, mapTransformIdToErrorEmitter.containsKey("transform2"));
    if (contains) {
      Assert.assertEquals(expected, mapTransformIdToErrorEmitter.get("transform2").size());
    }
  }

  private void assertResults(Map<String, DefaultEmitter> results, int expectedSink1Count, int expectedSink2Count) {
    Assert.assertEquals(2, results.size());
    DefaultEmitter sink1 = results.get("sink1");
    Iterator sink1Iterator = sink1.iterator();
    int sink1Count = 0, sink2Count = 0;
    while (sink1Iterator.hasNext()) {
      sink1Iterator.next();
      sink1Count++;
    }

    Assert.assertEquals(expectedSink1Count, sink1Count);

    DefaultEmitter sink2 = results.get("sink2");
    Iterator sink2Iterator = sink2.iterator();
    while (sink2Iterator.hasNext()) {
      sink2Iterator.next();
      sink2Count++;
    }

    Assert.assertEquals(expectedSink2Count, sink2Count);
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
        emitter.emitError(new InvalidEntry<>(100, "less than threshold", input));
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
