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

import io.cdap.cdap.app.runtime.ProgramRuntimeProvider;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.proto.ProgramType;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ServiceLoader;

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
    ClassLoader sparkClassLoader = runtimeProvider.getRuntimeClassLoader(ProgramType.SPARK, cConf);
    // Load any spark streaming class.
    sparkClassLoader.loadClass("org.apache.spark.streaming.StreamingContext");
    MasterEnvironments.setMasterEnvironment(tmpMasterEnv);
    ClassLoaders.setContextClassLoader(tmp);
  }
}
