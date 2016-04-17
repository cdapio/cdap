/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramRuntimeProvider;
import co.cask.cdap.app.runtime.spark.distributed.DistributedSparkProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.internal.app.runtime.spark.SparkUtils;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.inject.Injector;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A {@link ProgramRuntimeProvider} that provides runtime system support for {@link ProgramType#SPARK} program.
 * This class shouldn't have dependency on Spark classes.
 */
@ProgramRuntimeProvider.SupportedProgramType(ProgramType.SPARK)
public class SparkProgramRuntimeProvider implements ProgramRuntimeProvider {

  private URL[] classLoaderUrls;

  @Override
  public ProgramRunner createProgramRunner(ProgramType type, Mode mode, Injector injector) {
    Preconditions.checkArgument(type == ProgramType.SPARK, "Unsupported program type %s. Only %s is supported",
                                type, ProgramType.SPARK);
    switch (mode) {
      case LOCAL:
        return createSparkProgramRunner(injector, SparkProgramRunner.class.getName());
      case DISTRIBUTED:
        return createSparkProgramRunner(injector, DistributedSparkProgramRunner.class.getName());
      default:
        throw new IllegalArgumentException("Unsupported Spark execution mode " + mode);
    }
  }

  /**
   * Creates a {@link ProgramRunner} that execute Spark program from the given {@link Injector}.
   */
  private ProgramRunner createSparkProgramRunner(Injector injector, String programRunnerClassName) {
    try {
      boolean rewriteYarnClient = injector.getInstance(CConfiguration.class)
        .getBoolean(Constants.AppFabric.SPARK_YARN_CLIENT_REWRITE);
      final SparkRunnerClassLoader classLoader = new SparkRunnerClassLoader(getClassLoaderURLs(),
                                                                            getClass().getClassLoader(),
                                                                            rewriteYarnClient);
      try {
        ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(classLoader);
        try {
          // Closing of the SparkRunnerClassLoader is done by the SparkProgramRunner when the program execution finished
          // The current CDAP call run right after it get a ProgramRunner and never reuse a ProgramRunner.
          // TODO: CDAP-5506 to refactor the program runtime architecture to remove the need of this assumption
          return (ProgramRunner) injector.getInstance(classLoader.loadClass(programRunnerClassName));
        } finally {
          ClassLoaders.setContextClassLoader(oldClassLoader);
        }
      } catch (Throwable t) {
        // If there is any exception, close the SparkRunnerClassLoader
        Closeables.closeQuietly(classLoader);
        throw t;
      }
    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }

  private synchronized URL[] getClassLoaderURLs() throws IOException {
    if (classLoaderUrls != null) {
      return classLoaderUrls;
    }
    classLoaderUrls = getClassLoaderURLs(getClass().getClassLoader());
    return classLoaderUrls;
  }

  /**
   * Returns an array of {@link URL} that the given ClassLoader uses, including all URLs used by the parent of the
   * given ClassLoader. It will also includes the Spark aseembly jar if Spark classes are not loadable from
   * the given ClassLoader.
   */
  private URL[] getClassLoaderURLs(ClassLoader classLoader) throws IOException {
    List<URL> urls = getClassLoaderURLs(classLoader, new ArrayList<URL>());

    // If Spark classes are not available in the given ClassLoader, try to locate the Spark assembly jar
    // This class cannot have dependency on Spark directly, hence using the class resource to discover if SparkContext
    // is there
    if (classLoader.getResource("org/apache/spark/SparkContext.class") == null) {
      urls.add(SparkUtils.locateSparkAssemblyJar().toURI().toURL());
    }
    return urls.toArray(new URL[urls.size()]);
  }

  /**
   * Populates the list of {@link URL} that this ClassLoader uses, including all URLs used by the parent of the
   * given ClassLoader.
   */
  private List<URL> getClassLoaderURLs(ClassLoader classLoader, List<URL> urls) {
    if (classLoader == null) {
      return urls;
    }
    getClassLoaderURLs(classLoader.getParent(), urls);

    if (classLoader instanceof URLClassLoader) {
      urls.addAll(Arrays.asList(((URLClassLoader) classLoader).getURLs()));
    }
    return urls;
  }
}
