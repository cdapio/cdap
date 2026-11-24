/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import io.cdap.cdap.app.runtime.ProgramRuntimeProvider;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.runtime.spi.SparkCompat;
import java.io.Closeable;
import java.net.URLClassLoader;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

/**
 * Test for {@link SparkProgramRuntimeProvider}. We need to use the delegated test runner to ensure that there aren't
 * any casting errors for CDAP classes due to using different effective classloaders at runtime.
 */
@RunWith(NoSparkClassLoaderTestRunner.class)
public class SparkProgramRuntimeProviderTest {

  @Test
  public void testSparkResourceFilterStreamingClassLoading() throws Exception {
    // Tests the scenario in which the parent classloader of the SparkProgramRuntimeProvider does not have access to
    // Spark classes. We simulate this by wrapping the entire test class with a delegated classloader which filters out
    // Spark classes.

    // Set the thread context classloader as a delegate classloader which does not have the Spark classes.
    // This simulates that the parent classloader of SparkProgramRuntimeProvider does not have Spark classes.
    ClassLoader noSparkClassLoader = getClass().getClassLoader();
    ClassLoader tmp = ClassLoaders.setContextClassLoader(noSparkClassLoader);
    MasterEnvironment masterEnvironment = new MockMasterEnvironment();
    masterEnvironment.initialize(null);
    MasterEnvironment tmpMasterEnv = MasterEnvironments.setMasterEnvironment(masterEnvironment);

    CConfiguration cConf = CConfiguration.create();
    ServiceLoader<ProgramRuntimeProvider> serviceLoader = ServiceLoader.load(ProgramRuntimeProvider.class,
                                                                             noSparkClassLoader);
    SparkProgramRuntimeProvider runtimeProvider = (SparkProgramRuntimeProvider) serviceLoader.iterator().next();
    ClassLoader sparkClassLoader = runtimeProvider.createProgramClassLoader(cConf, ProgramType.SPARK);
    // Load any spark streaming class.
    sparkClassLoader.loadClass("org.apache.spark.streaming.StreamingContext");
    MasterEnvironments.setMasterEnvironment(tmpMasterEnv);
    ClassLoaders.setContextClassLoader(tmp);
  }

  @Test
  public void testCreateProgramRunnerClosesClassLoaderOnFailureAndPropagates() throws Exception {
    SparkProgramRuntimeProvider provider = new SparkProgramRuntimeProvider(SparkCompat.SPARK3_2_12) {
    };
    CConfiguration cConf = CConfiguration.create();
    final AtomicReference<ClassLoader> capturedRunnerClassLoader = new AtomicReference<>();
    final IllegalStateException expectedCause = new IllegalStateException("simulated guice failure");

    Injector failingInjector = Guice.createInjector(
        new ConfigModule(cConf, new Configuration()),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(LocationFactory.class).toInstance(Mockito.mock(LocationFactory.class));
            bind(TransactionSystemClient.class).toInstance(Mockito.mock(TransactionSystemClient.class));
          }

          @Provides
          public DatasetFramework provideDatasetFramework() {
            capturedRunnerClassLoader.set(Thread.currentThread().getContextClassLoader());
            throw expectedCause;
          }
        });

    ClassLoader originalContextCl = Thread.currentThread().getContextClassLoader();
    try {
      provider.createProgramRunner(ProgramType.SPARK, ProgramRuntimeProvider.Mode.LOCAL, failingInjector);
      Assert.fail("Expected RuntimeException when runner instantiation fails");
    } catch (RuntimeException caught) {
      // ProvisionException is an unchecked RuntimeException and must be propagated without double-wrapping.
      Assert.assertSame(expectedCause, caught.getCause());
    }

    Assert.assertSame(originalContextCl, Thread.currentThread().getContextClassLoader());
    ClassLoader runnerCl = capturedRunnerClassLoader.get();
    Assert.assertTrue(runnerCl instanceof URLClassLoader);

    // Verify an unclosed SparkRunnerClassLoader loads org.apache.spark.internal.Logging cleanly, whereas
    // the SparkRunnerClassLoader closed via Closeables.closeQuietly(classLoader) on failure throws
    // ClassNotFoundException when findClass attempts to read from closed jar URLs.
    ClassLoader openRunnerCl = provider.createProgramClassLoader(cConf, ProgramType.SPARK);
    Assert.assertNotNull(openRunnerCl.loadClass("org.apache.spark.internal.Logging"));
    ((Closeable) openRunnerCl).close();

    try {
      runnerCl.loadClass("org.apache.spark.internal.Logging");
      Assert.fail("Expected ClassNotFoundException from closed SparkRunnerClassLoader");
    } catch (ClassNotFoundException expected) {
      // expected
    }
  }
}
