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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.internal.app.runtime.plugin.PluginClassLoaders;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.spark.executor.ExecutorURLClassLoader;
import org.apache.spark.util.MutableURLClassLoader;

import java.util.List;

/**
 * ClassLoader being used in Spark execution context. It is used in driver as well as in executor node.
 * It load classes from {@link ProgramClassLoader} followed by CDAP system ClassLoader.
 */
public class SparkClassLoader extends CombineClassLoader {

  private final ClassLoader programClassLoader;
  private final ExecutionSparkContext context;

  /**
   * Finds the SparkClassLoader from the context ClassLoader hierarchy.
   *
   * @return the SparkClassLoader found
   * @throws IllegalStateException if no SparkClassLoader was found
   */
  public static SparkClassLoader findFromContext() {
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    SparkClassLoader sparkClassLoader = ClassLoaders.find(contextClassLoader,
                                                          SparkClassLoader.class);
    // Should found the Spark ClassLoader
    Preconditions.checkState(sparkClassLoader != null, "Cannot find SparkClassLoader from context ClassLoader %s",
                             contextClassLoader);
    return sparkClassLoader;
  }

  /**
   * Creates a new SparkClassLoader from the execution context. It should only be called from
   * {@link ExecutorURLClassLoader} or {@link MutableURLClassLoader} when invoked in the executor container only.
   */
  public static SparkClassLoader create() {
    ExecutionSparkContext sparkContext = SparkContextProvider.getSparkContext();
    return new SparkClassLoader(sparkContext);
  }

  /**
   * Creates a new SparkClassLoader with the given {@link ExecutionSparkContext}.
   */
  public SparkClassLoader(ExecutionSparkContext context) {
    super(null, createDelegateClassLoaders(context));
    this.programClassLoader = context.getProgramClassLoader();
    this.context = context;
  }

  /**
   * Returns the program ClassLoader.
   */
  public ClassLoader getProgramClassLoader() {
    return programClassLoader;
  }

  /**
   * Returns the {@link ExecutionSparkContext}.
   */
  public ExecutionSparkContext getContext() {
    return context;
  }

  /**
   * Creates the delegating list of ClassLoader. Used by constructor only.
   */
  private static List<ClassLoader> createDelegateClassLoaders(ExecutionSparkContext context) {
    return ImmutableList.of(
      context.getProgramClassLoader(),
      PluginClassLoaders.createFilteredPluginsClassLoader(context.getApplicationSpecification().getPlugins(),
                                                          context.getPluginInstantiator()),
      SparkClassLoader.class.getClassLoader()
    );
  }
}
