/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.batch;

import io.cdap.cdap.api.ProgramLifecycle;
import io.cdap.cdap.api.annotation.UseDataSet;
import io.cdap.cdap.api.dataset.Dataset;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ReducerWrapper}.
 */
public class ReducerWrapperTest {

  private static final IllegalArgumentException CTOR_FAILURE =
      new IllegalArgumentException("reducer constructor boom");
  private static Exception lifecycleInitFailure;

  /**
   * Reducer whose constructor throws an unchecked exception.
   */
  public static final class FailingCtorReducer extends Reducer<Object, Object, Object, Object> {
    public FailingCtorReducer() {
      throw CTOR_FAILURE;
    }
  }

  /**
   * Reducer with a {@link UseDataSet} field to exercise field injection failure propagation.
   */
  public static final class DatasetInjectedReducer extends Reducer<Object, Object, Object, Object> {
    @SuppressWarnings("unused")
    @UseDataSet("missingDs")
    private Dataset dataset;
  }

  /**
   * Reducer implementing {@link ProgramLifecycle} to exercise {@code initialize} propagation.
   */
  public static final class LifecycleReducer extends Reducer<Object, Object, Object, Object>
      implements ProgramLifecycle<MapReduceLifecycleContext> {

    @Override
    public void initialize(MapReduceLifecycleContext context) throws Exception {
      if (lifecycleInitFailure != null) {
        throw lifecycleInitFailure;
      }
    }

    @Override
    public void run(Context context) {
      // no-op
    }

    @Override
    public void destroy() {
      // no-op
    }
  }

  /**
   * Reducer that invokes {@code context.nextKeyValue()} once to trigger periodic flushing.
   */
  public static final class AutoFlushingReducer extends Reducer<Object, Object, Object, Object> {
    @Override
    public void run(Context context) throws IOException, InterruptedException {
      context.nextKeyValue();
    }
  }

  @Test
  public void testWrapSetsReducerWrapperWhenDefined() throws Exception {
    Job job = Job.getInstance(new Configuration());
    job.setReducerClass(AutoFlushingReducer.class);
    ReducerWrapper.wrap(job);

    Assert.assertEquals(ReducerWrapper.class, job.getReducerClass());
    Assert.assertEquals(
        AutoFlushingReducer.class.getName(), job.getConfiguration().get("c.reducer.class"));
  }

  @Test
  public void testCreateReducerInstancePropagatesUncheckedAndWrapsChecked() throws Exception {
    ReducerWrapper wrapper = new ReducerWrapper();

    Reducer.Context ctorFailCtx = createHadoopContext(
        FailingCtorReducer.class.getName(), 10000, MapperWrapperTest.createMockTaskContext());
    try {
      wrapper.run(ctorFailCtx);
      Assert.fail("Expected IllegalArgumentException unwrapped");
    } catch (RuntimeException thrown) {
      Assert.assertSame(CTOR_FAILURE, thrown);
    }

    Reducer.Context missingCtx = createHadoopContext(
        "io.cdap.cdap.nonexistent.MissingReducer",
        10000,
        MapperWrapperTest.createMockTaskContext());
    try {
      wrapper.run(missingCtx);
      Assert.fail("Expected RuntimeException wrapping ClassNotFoundException");
    } catch (RuntimeException thrown) {
      Assert.assertTrue(thrown.getCause() instanceof ClassNotFoundException);
    }
  }

  @Test
  public void testFieldInjectionAndLifecycleInitializePropagateUncheckedUnwrapped()
      throws Exception {
    ReducerWrapper wrapper = new ReducerWrapper();

    BasicMapReduceTaskContext<?, ?> injectTaskContext = MapperWrapperTest.createMockTaskContext();
    IllegalStateException dsFailure = new IllegalStateException("dataset lookup failed");
    Mockito.when(injectTaskContext.getDataset("missingDs")).thenThrow(dsFailure);
    Reducer.Context injectCtx = createHadoopContext(
        DatasetInjectedReducer.class.getName(), 10000, injectTaskContext);
    try {
      wrapper.run(injectCtx);
      Assert.fail("Expected IllegalStateException unwrapped");
    } catch (RuntimeException thrown) {
      Assert.assertSame(dsFailure, thrown);
    }

    IllegalStateException initUnchecked = new IllegalStateException("reducer init unchecked");
    lifecycleInitFailure = initUnchecked;
    Reducer.Context initUncheckedCtx = createHadoopContext(
        LifecycleReducer.class.getName(), 10000, MapperWrapperTest.createMockTaskContext());
    try {
      wrapper.run(initUncheckedCtx);
      Assert.fail("Expected IllegalStateException unwrapped");
    } catch (RuntimeException thrown) {
      Assert.assertSame(initUnchecked, thrown);
    }

    Exception initChecked = new Exception("reducer init checked");
    lifecycleInitFailure = initChecked;
    try {
      wrapper.run(initUncheckedCtx);
      Assert.fail("Expected RuntimeException wrapping checked exception");
    } catch (RuntimeException thrown) {
      Assert.assertSame(initChecked, thrown.getCause());
    } finally {
      lifecycleInitFailure = null;
    }
  }

  @Test
  public void testEndOfRunAndAutoFlushPropagateUncheckedAndWrapChecked() throws Exception {
    ReducerWrapper wrapper = new ReducerWrapper();
    lifecycleInitFailure = null;

    // End-of-reducer flushOperations()
    BasicMapReduceTaskContext<?, ?> endFlushTaskCtx = MapperWrapperTest.createMockTaskContext();
    IllegalStateException endUnchecked = new IllegalStateException("end flush unchecked");
    Mockito.doThrow(endUnchecked).when(endFlushTaskCtx).flushOperations();
    Reducer.Context endFlushCtx =
        createHadoopContext(LifecycleReducer.class.getName(), 10000, endFlushTaskCtx);
    try {
      wrapper.run(endFlushCtx);
      Assert.fail("Expected IllegalStateException unwrapped");
    } catch (RuntimeException thrown) {
      Assert.assertSame(endUnchecked, thrown);
    }

    Exception endChecked = new Exception("end flush checked");
    Mockito.doThrow(endChecked).when(endFlushTaskCtx).flushOperations();
    try {
      wrapper.run(endFlushCtx);
      Assert.fail("Expected RuntimeException wrapping checked exception");
    } catch (RuntimeException thrown) {
      Assert.assertSame(endChecked, thrown.getCause());
    }

    // Periodic auto-flush inside nextKeyValue() when flushFreq = 0
    BasicMapReduceTaskContext<?, ?> autoFlushTaskCtx = MapperWrapperTest.createMockTaskContext();
    IllegalArgumentException autoUnchecked = new IllegalArgumentException("auto flush unchecked");
    Mockito.doThrow(autoUnchecked).when(autoFlushTaskCtx).flushOperations();
    Reducer.Context autoFlushCtx =
        createHadoopContext(AutoFlushingReducer.class.getName(), 0, autoFlushTaskCtx);
    Mockito.when(autoFlushCtx.nextKey()).thenReturn(false);
    try {
      wrapper.run(autoFlushCtx);
      Assert.fail("Expected IllegalArgumentException unwrapped");
    } catch (RuntimeException thrown) {
      Assert.assertSame(autoUnchecked, thrown);
    }

    Exception autoChecked = new Exception("auto flush checked");
    Mockito.doThrow(autoChecked).when(autoFlushTaskCtx).flushOperations();
    try {
      wrapper.run(autoFlushCtx);
      Assert.fail("Expected RuntimeException wrapping checked exception");
    } catch (RuntimeException thrown) {
      Assert.assertSame(autoChecked, thrown.getCause());
    }
  }

  @SuppressWarnings({"rawtypes"})
  private Reducer.Context createHadoopContext(
      String reducerClassName, int flushFreq, BasicMapReduceTaskContext taskContext)
      throws Exception {
    Configuration conf = new Configuration();
    conf.set("c.reducer.class", reducerClassName);
    conf.setInt("c.reducer.flush.freq", flushFreq);

    MapReduceClassLoader mrClassLoader = Mockito.mock(MapReduceClassLoader.class);
    MapReduceTaskContextProvider provider =
        MapperWrapperTest.createFixedProvider(mrClassLoader, taskContext);
    Mockito.when(mrClassLoader.getTaskContextProvider()).thenReturn(provider);
    Mockito.when(mrClassLoader.getProgramClassLoader()).thenReturn(getClass().getClassLoader());
    conf.setClassLoader(mrClassLoader);

    Reducer.Context hadoopContext = Mockito.mock(Reducer.Context.class);
    Mockito.when(hadoopContext.getConfiguration()).thenReturn(conf);
    return hadoopContext;
  }
}
