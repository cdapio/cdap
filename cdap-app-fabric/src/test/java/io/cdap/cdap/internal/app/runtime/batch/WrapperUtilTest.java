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
import io.cdap.cdap.common.lang.Delegator;
import java.net.URL;
import java.net.URLClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link WrapperUtil}.
 */
public class WrapperUtilTest {

  private static Exception initFailure;
  private static ClassLoader observedInitClassLoader;

  /**
   * Plain delegate that does not implement {@link ProgramLifecycle}.
   */
  public static final class PlainDelegate {
  }

  /**
   * Delegate implementing {@link ProgramLifecycle}.
   */
  public static final class LifecycleDelegate
      implements ProgramLifecycle<MapReduceLifecycleContext> {

    @Override
    public void initialize(MapReduceLifecycleContext context) throws Exception {
      observedInitClassLoader = Thread.currentThread().getContextClassLoader();
      if (initFailure != null) {
        throw initFailure;
      }
    }

    @Override
    public void destroy() {
      // no-op
    }
  }

  @Test
  public void testCreatePlainDelegateAndSetIfDefined() throws Exception {
    Configuration conf = new Configuration();
    conf.set("delegate.attr", PlainDelegate.class.getName());
    PlainDelegate plain = WrapperUtil.createDelegate(conf, "delegate.attr");
    Assert.assertNotNull(plain);

    Job job = Job.getInstance(new Configuration());
    Assert.assertFalse(WrapperUtil.setIfDefined(job, "missing.key", "dest.key"));
    job.getConfiguration().set("present.key", "val123");
    Assert.assertTrue(WrapperUtil.setIfDefined(job, "present.key", "dest.key"));
    Assert.assertEquals("val123", job.getConfiguration().get("dest.key"));
  }

  @Test
  public void testCreateLifecycleDelegateSuccessAndExceptionPropagation() throws Exception {
    BasicMapReduceTaskContext<?, ?> taskContext = Mockito.mock(BasicMapReduceTaskContext.class);
    try (URLClassLoader programClassLoader =
        new URLClassLoader(new URL[0], getClass().getClassLoader())) {
      Configuration conf = createLifecycleConfiguration(taskContext, programClassLoader);
      final ClassLoader callerClassLoader = Thread.currentThread().getContextClassLoader();

      initFailure = null;
      observedInitClassLoader = null;
      LifecycleDelegate delegate = WrapperUtil.createDelegate(conf, "delegate.attr");
      Assert.assertNotNull(delegate);
      Assert.assertSame(programClassLoader, observedInitClassLoader);
      Assert.assertSame(callerClassLoader, Thread.currentThread().getContextClassLoader());
      Mockito.verify(taskContext).registerProgramLifecycle(delegate);

      // Unchecked exception must be propagated unwrapped and context ClassLoader restored
      IllegalStateException unchecked = new IllegalStateException("delegate init unchecked");
      initFailure = unchecked;
      try {
        WrapperUtil.createDelegate(conf, "delegate.attr");
        Assert.fail("Expected IllegalStateException unwrapped");
      } catch (RuntimeException thrown) {
        Assert.assertSame(unchecked, thrown);
      }
      Assert.assertSame(callerClassLoader, Thread.currentThread().getContextClassLoader());

      // Checked exception must be wrapped in RuntimeException and context ClassLoader restored
      Exception checked = new Exception("delegate init checked");
      initFailure = checked;
      try {
        WrapperUtil.createDelegate(conf, "delegate.attr");
        Assert.fail("Expected RuntimeException wrapping checked exception");
      } catch (RuntimeException thrown) {
        Assert.assertSame(checked, thrown.getCause());
      } finally {
        initFailure = null;
      }
      Assert.assertSame(callerClassLoader, Thread.currentThread().getContextClassLoader());
    }
  }

  @SuppressWarnings({"rawtypes"})
  private Configuration createLifecycleConfiguration(
      BasicMapReduceTaskContext taskContext, ClassLoader programClassLoader) throws Exception {
    Configuration conf = new Configuration();
    conf.set("delegate.attr", LifecycleDelegate.class.getName());

    MapReduceClassLoader mrClassLoader = Mockito.mock(MapReduceClassLoader.class);
    MapReduceTaskContextProvider provider =
        MapperWrapperTest.createFixedProvider(mrClassLoader, taskContext);
    Mockito.when(mrClassLoader.getTaskContextProvider()).thenReturn(provider);
    Mockito.when(mrClassLoader.getProgramClassLoader()).thenReturn(programClassLoader);
    conf.setClassLoader(new DelegatingMapReduceClassLoader(programClassLoader, mrClassLoader));
    return conf;
  }

  /**
   * Configuration ClassLoader that loads classes through the program ClassLoader (its parent) and
   * exposes the mocked {@link MapReduceClassLoader} via {@link Delegator}, which is how
   * {@link MapReduceClassLoader#getFromConfiguration} finds it. Setting the mock itself as the
   * Configuration ClassLoader does not load classes on Java 11.
   */
  private static final class DelegatingMapReduceClassLoader extends ClassLoader
      implements Delegator<ClassLoader> {

    private final MapReduceClassLoader delegate;

    DelegatingMapReduceClassLoader(ClassLoader parent, MapReduceClassLoader delegate) {
      super(parent);
      this.delegate = delegate;
    }

    @Override
    public ClassLoader getDelegate() {
      return delegate;
    }
  }
}
