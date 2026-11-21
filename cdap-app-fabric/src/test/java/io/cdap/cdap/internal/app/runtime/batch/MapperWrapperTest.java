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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Injector;
import io.cdap.cdap.api.mapreduce.MapReduceSpecification;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link MapperWrapper}.
 */
public class MapperWrapperTest {

  private static final IllegalArgumentException CTOR_FAILURE =
      new IllegalArgumentException("mapper constructor boom");

  /**
   * Mapper whose constructor throws an unchecked exception.
   */
  public static final class FailingCtorMapper extends Mapper<Object, Object, Object, Object> {
    public FailingCtorMapper() {
      throw CTOR_FAILURE;
    }
  }

  /**
   * Mapper that triggers {@code context.nextKeyValue()} once to exercise auto-flushing.
   */
  public static final class SingleStepMapper extends Mapper<Object, Object, Object, Object> {
    @Override
    public void run(Context context) throws IOException, InterruptedException {
      context.nextKeyValue();
    }
  }

  @Test
  public void testWrapAndGetWrappedMapper() throws Exception {
    Job job = Job.getInstance(new Configuration());
    job.setMapperClass(SingleStepMapper.class);
    MapperWrapper.wrap(job);

    Assert.assertEquals(MapperWrapper.class, job.getMapperClass());
    Assert.assertEquals(
        SingleStepMapper.class.getName(), MapperWrapper.getWrappedMapper(job.getConfiguration()));
  }

  @Test
  public void testCreateMapperInstancePropagatesUncheckedAndWrapsChecked() throws Exception {
    Mapper.Context hadoopContext = createHadoopContext(FailingCtorMapper.class.getName(), 10000);
    MapperWrapper wrapper = new MapperWrapper();

    try {
      wrapper.run(hadoopContext);
      Assert.fail("Expected IllegalArgumentException unwrapped");
    } catch (RuntimeException thrown) {
      Assert.assertSame(CTOR_FAILURE, thrown);
    }

    Mapper.Context missingClassContext =
        createHadoopContext("io.cdap.cdap.nonexistent.MissingMapper", 10000);
    try {
      wrapper.run(missingClassContext);
      Assert.fail("Expected RuntimeException wrapping ClassNotFoundException");
    } catch (RuntimeException thrown) {
      Assert.assertTrue(thrown.getCause() instanceof ClassNotFoundException);
    }
  }

  @Test
  public void testAutoFlushingContextPropagatesUncheckedAndWrapsChecked() throws Exception {
    BasicMapReduceTaskContext<?, ?> taskContext = createMockTaskContext();
    Mapper.Context hadoopContext =
        createHadoopContextWithTaskContext(SingleStepMapper.class.getName(), 0, taskContext);
    Mockito.when(hadoopContext.nextKeyValue()).thenReturn(false);

    IllegalStateException uncheckedFlush = new IllegalStateException("mapper auto-flush boom");
    Mockito.doThrow(uncheckedFlush).when(taskContext).flushOperations();

    MapperWrapper wrapper = new MapperWrapper();
    try {
      wrapper.run(hadoopContext);
      Assert.fail("Expected IllegalStateException unwrapped");
    } catch (RuntimeException thrown) {
      Assert.assertSame(uncheckedFlush, thrown);
    }

    Exception checkedFlush = new Exception("mapper checked flush boom");
    Mockito.doThrow(checkedFlush).when(taskContext).flushOperations();
    try {
      wrapper.run(hadoopContext);
      Assert.fail("Expected RuntimeException wrapping checked exception");
    } catch (RuntimeException thrown) {
      Assert.assertSame(checkedFlush, thrown.getCause());
    }
  }

  private Mapper.Context createHadoopContext(String mapperClassName, int flushFreq)
      throws Exception {
    return createHadoopContextWithTaskContext(
        mapperClassName, flushFreq, createMockTaskContext());
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private Mapper.Context createHadoopContextWithTaskContext(
      String mapperClassName, int flushFreq, BasicMapReduceTaskContext taskContext)
      throws Exception {
    Configuration conf = new Configuration();
    conf.set("c.mapper.class", mapperClassName);
    conf.setInt("c.mapper.flush.freq", flushFreq);

    MapReduceClassLoader mrClassLoader = Mockito.mock(MapReduceClassLoader.class);
    MapReduceTaskContextProvider provider = createFixedProvider(mrClassLoader, taskContext);
    Mockito.when(mrClassLoader.getTaskContextProvider()).thenReturn(provider);
    Mockito.when(mrClassLoader.getProgramClassLoader()).thenReturn(getClass().getClassLoader());
    conf.setClassLoader(mrClassLoader);

    Mapper.Context hadoopContext = Mockito.mock(Mapper.Context.class);
    Mockito.when(hadoopContext.getConfiguration()).thenReturn(conf);
    return hadoopContext;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  static MapReduceTaskContextProvider createFixedProvider(
      MapReduceClassLoader mrClassLoader, BasicMapReduceTaskContext taskContext) throws Exception {
    Injector injector = Mockito.mock(Injector.class, invocation -> {
      if ("getInstance".equals(invocation.getMethod().getName())
          && invocation.getArguments().length == 1
          && invocation.getArguments()[0] instanceof Class) {
        return Mockito.mock((Class<?>) invocation.getArguments()[0]);
      }
      return Mockito.RETURNS_DEFAULTS.answer(invocation);
    });
    MapReduceTaskContextProvider provider =
        new MapReduceTaskContextProvider(injector, mrClassLoader);
    LoadingCache<Object, BasicMapReduceTaskContext> fixedCache = CacheBuilder.newBuilder()
        .build(new CacheLoader<Object, BasicMapReduceTaskContext>() {
          @Override
          public BasicMapReduceTaskContext load(Object key) {
            return taskContext;
          }
        });
    Field cacheField = MapReduceTaskContextProvider.class.getDeclaredField("taskContexts");
    cacheField.setAccessible(true);
    cacheField.set(provider, fixedCache);
    return provider;
  }

  static BasicMapReduceTaskContext<?, ?> createMockTaskContext() {
    BasicMapReduceTaskContext<?, ?> taskContext = Mockito.mock(BasicMapReduceTaskContext.class);
    MapReduceSpecification spec = new MapReduceSpecification(
        "io.cdap.cdap.DummyMr", "mrProg", "desc", null, null,
        Collections.emptySet(), Collections.emptyMap(), null, null, null, Collections.emptyMap());
    Mockito.when(taskContext.getProgramName()).thenReturn("mrProg");
    Mockito.when(taskContext.getSpecification()).thenReturn(spec);
    Mockito.when(taskContext.getMetrics()).thenReturn(Mockito.mock(Metrics.class));
    Mockito.when(taskContext.getProgramMetrics()).thenReturn(
        new NoOpMetricsCollectionService().getContext(Collections.emptyMap()));
    Mockito.when(taskContext.getMetricsReportIntervalMillis()).thenReturn(60_000L);
    return taskContext;
  }
}
