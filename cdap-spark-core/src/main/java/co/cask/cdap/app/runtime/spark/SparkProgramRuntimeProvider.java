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
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.inject.Injector;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A {@link ProgramRuntimeProvider} that provides runtime system support for {@link ProgramType#SPARK} program.
 */
@ProgramRuntimeProvider.SupportedProgramType(ProgramType.SPARK)
public class SparkProgramRuntimeProvider implements ProgramRuntimeProvider {

  private static final String SPARK_PROGRAM_RUNNER_CLASS_NAME = SparkProgramRunner.class.getName();
  private final URL[] classLoaderUrls;

  public SparkProgramRuntimeProvider() {
    this.classLoaderUrls = getClassLoaderURLs(getClass().getClassLoader());
  }

  @Override
  public ProgramRunner createProgramRunner(ProgramType type, Mode mode, Injector injector) {
    Preconditions.checkArgument(type == ProgramType.SPARK, "Unsupported program type %s. Only %s is supported",
                                type, ProgramType.SPARK);
    switch (mode) {
      case LOCAL:
        return createSparkProgramRunner(injector);
      case DISTRIBUTED:
        return injector.getInstance(DistributedSparkProgramRunner.class);
      default:
        throw new IllegalArgumentException("Unsupported Spark execution mode " + mode);
    }
  }


  @Override
  public FilterClassLoader.Filter createProgramClassLoaderFilter(ProgramType programType) {
    return SparkProgramRunner.SPARK_PROGRAM_CLASS_LOADER_FILTER;
  }

  /**
   * Creates a {@link ProgramRunner} that execute Spark program from the given {@link Injector}.
   */
  public ProgramRunner createSparkProgramRunner(Injector injector) {
    final SparkRunnerClassLoader classLoader = new SparkRunnerClassLoader(classLoaderUrls, getClass().getClassLoader());
    try {
      ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(classLoader);
      try {
        // Closing of the SparkRunnerClassLoader is done by the SparkProgramRunner when the program execution finished
        // The current CDAP call run right after it get a ProgramRunner and never reuse a ProgramRunner.
        // TODO: CDAP-5506 to refactor the program runtime architecture to remove the need of this assumption
        return (ProgramRunner) injector.getInstance(classLoader.loadClass(SPARK_PROGRAM_RUNNER_CLASS_NAME));
      } finally {
        ClassLoaders.setContextClassLoader(oldClassLoader);
      }
    } catch (Throwable t) {
      // If there is any exception, close the SparkRunnerClassLoader
      Closeables.closeQuietly(classLoader);
      throw Throwables.propagate(t);
    }
  }

  /**
   * Returns an array of {@link URL} that the given ClassLoader uses, including all URLs used by the parent of the
   * given ClassLoader.
   */
  private URL[] getClassLoaderURLs(ClassLoader classLoader) {
    List<URL> urls = getClassLoaderURLs(classLoader, new ArrayList<URL>());
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
