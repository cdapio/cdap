/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.etl.batch.mapreduce;

import io.cdap.cdap.etl.exec.StageFailureException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Iterator;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ETLMapReduce}.
 */
public class ETLMapReduceTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testMapperPropagatesStageFailureAndGeneralExceptions() throws Exception {
    TransformRunner<Object, Object> runner = Mockito.mock(TransformRunner.class);
    ETLMapReduce.ETLMapper mapper = new ETLMapReduce.ETLMapper();
    injectTransformRunner(mapper, runner);

    IOException checkedCause = new IOException("stage checked cause");
    Mockito.doThrow(createStageFailure("stage1", checkedCause))
        .when(runner).transform("k1", "v1");
    try {
      mapper.map("k1", "v1", null);
      Assert.fail("Expected RuntimeException wrapping checked cause");
    } catch (RuntimeException actual) {
      Assert.assertSame(checkedCause, actual.getCause());
    }

    IllegalArgumentException uncheckedCause = new IllegalArgumentException("stage unchecked cause");
    Mockito.doThrow(createStageFailure("stage1", uncheckedCause))
        .when(runner).transform("k2", "v2");
    try {
      mapper.map("k2", "v2", null);
      Assert.fail("Expected IllegalArgumentException directly");
    } catch (IllegalArgumentException actual) {
      Assert.assertSame(uncheckedCause, actual);
    }

    IOException directChecked = new IOException("direct checked failure");
    Mockito.doThrow(directChecked).when(runner).transform("k3", "v3");
    try {
      mapper.map("k3", "v3", null);
      Assert.fail("Expected RuntimeException wrapping direct IOException");
    } catch (RuntimeException actual) {
      Assert.assertSame(directChecked, actual.getCause());
    }

    IllegalStateException directUnchecked = new IllegalStateException("direct unchecked failure");
    Mockito.doThrow(directUnchecked).when(runner).transform("k4", "v4");
    try {
      mapper.map("k4", "v4", null);
      Assert.fail("Expected IllegalStateException directly");
    } catch (IllegalStateException actual) {
      Assert.assertSame(directUnchecked, actual);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testReducerPropagatesStageFailureAndGeneralExceptions() throws Exception {
    TransformRunner<Object, Iterator> runner = Mockito.mock(TransformRunner.class);
    ETLMapReduce.ETLReducer reducer = new ETLMapReduce.ETLReducer();
    injectTransformRunner(reducer, runner);

    IOException checkedCause = new IOException("reduce stage checked cause");
    Mockito.doThrow(createStageFailure("stage1", checkedCause))
        .when(runner).transform(Mockito.eq("k1"), Mockito.any(Iterator.class));
    try {
      reducer.reduce("k1", Collections.singletonList("v1"), null);
      Assert.fail("Expected RuntimeException wrapping checked cause");
    } catch (RuntimeException actual) {
      Assert.assertSame(checkedCause, actual.getCause());
    }

    IllegalArgumentException uncheckedCause = new IllegalArgumentException("reduce unchecked cause");
    Mockito.doThrow(createStageFailure("stage1", uncheckedCause))
        .when(runner).transform(Mockito.eq("k2"), Mockito.any(Iterator.class));
    try {
      reducer.reduce("k2", Collections.singletonList("v2"), null);
      Assert.fail("Expected IllegalArgumentException directly");
    } catch (IllegalArgumentException actual) {
      Assert.assertSame(uncheckedCause, actual);
    }

    IOException directChecked = new IOException("reduce direct checked failure");
    Mockito.doThrow(directChecked)
        .when(runner).transform(Mockito.eq("k3"), Mockito.any(Iterator.class));
    try {
      reducer.reduce("k3", Collections.singletonList("v3"), null);
      Assert.fail("Expected RuntimeException wrapping direct IOException");
    } catch (RuntimeException actual) {
      Assert.assertSame(directChecked, actual.getCause());
    }

    IllegalStateException directUnchecked = new IllegalStateException("reduce direct unchecked");
    Mockito.doThrow(directUnchecked)
        .when(runner).transform(Mockito.eq("k4"), Mockito.any(Iterator.class));
    try {
      reducer.reduce("k4", Collections.singletonList("v4"), null);
      Assert.fail("Expected IllegalStateException directly");
    } catch (IllegalStateException actual) {
      Assert.assertSame(directUnchecked, actual);
    }
  }

  private StageFailureException createStageFailure(String stage, Throwable cause) throws Exception {
    Constructor<StageFailureException> ctor =
        StageFailureException.class.getDeclaredConstructor(String.class, Throwable.class);
    ctor.setAccessible(true);
    return ctor.newInstance(stage, cause);
  }

  private void injectTransformRunner(Object target, TransformRunner<?, ?> runner) throws Exception {
    Field field = target.getClass().getDeclaredField("transformRunner");
    field.setAccessible(true);
    field.set(target, runner);
  }
}
