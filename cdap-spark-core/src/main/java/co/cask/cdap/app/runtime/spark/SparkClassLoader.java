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

import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.SparkExecutionContext;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.internal.app.runtime.plugin.PluginClassLoaders;
import com.google.common.base.Preconditions;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ClassLoader being used in Spark execution context. It is used in driver as well as in executor node.
 * It load classes from {@link ProgramClassLoader} followed by Plugin classes and then CDAP system ClassLoader.
 */
public class SparkClassLoader extends CombineClassLoader {

  private final SparkRuntimeContext runtimeContext;
  private final SparkExecutionContextFactory contextFactory;

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
   * Creates a new SparkClassLoader from the execution context. It should only be called in distributed mode.
   */
  public static SparkClassLoader create() {
    return new SparkClassLoader(SparkRuntimeContextProvider.get(), createSparkExecutionContextFactory());
  }

  /**
   * Creates a new SparkClassLoader with the given {@link SparkRuntimeContext}.
   */
  public SparkClassLoader(SparkRuntimeContext runtimeContext, SparkExecutionContextFactory contextFactory) {
    super(null, createDelegateClassLoaders(runtimeContext));
    this.runtimeContext = runtimeContext;
    this.contextFactory = contextFactory;
  }

  /**
   * Returns the program ClassLoader.
   */
  public ClassLoader getProgramClassLoader() {
    return runtimeContext.getProgram().getClassLoader();
  }

  /**
   * Returns the {@link SparkRuntimeContext}.
   */
  public SparkRuntimeContext getRuntimeContext() {
    return runtimeContext;
  }

  /**
   * Creates a new instance of {@link SparkExecutionContext}.
   */
  public SparkExecutionContext createExecutionContext() {
    return contextFactory.create(runtimeContext);
  }

  /**
   * Creates a new instance of {@link JavaSparkExecutionContext} by wrapping the given {@link SparkExecutionContext}.
   */
  public JavaSparkExecutionContext createJavaExecutionContext(SparkExecutionContext sec) {
    return new DefaultJavaSparkExecutionContext(sec);
  }

  /**
   * Creates the delegating list of ClassLoader. Used by constructor only.
   */
  private static List<ClassLoader> createDelegateClassLoaders(SparkRuntimeContext context) {
    return Arrays.asList(
      context.getProgram().getClassLoader(),
      PluginClassLoaders.createFilteredPluginsClassLoader(context.getApplicationSpecification().getPlugins(),
                                                          context.getPluginInstantiator()),
      SparkClassLoader.class.getClassLoader()
    );
  }

  /**
   * Creates a {@link SparkExecutionContextFactory} to be used in distributed mode.
   */
  private static SparkExecutionContextFactory createSparkExecutionContextFactory() {
    return new SparkExecutionContextFactory() {
      @Override
      public SparkExecutionContext create(SparkRuntimeContext runtimeContext) {
        SparkRuntimeContextConfig contextConfig = new SparkRuntimeContextConfig(runtimeContext.getConfiguration());

        Map<String, File> localizeResources = new HashMap<>();
        for (String name : contextConfig.getLocalizedResourceNames()) {
          // In distributed mode, files will be localized to the container local directory
          localizeResources.put(name, new File(name));
        }

        return new DefaultSparkExecutionContext(runtimeContext, localizeResources);
      }
    };
  }
}

