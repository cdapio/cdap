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
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.api.spark.SparkExecutionContext;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.internal.app.runtime.ProgramClassLoader;
import co.cask.cdap.internal.app.runtime.plugin.PluginClassLoaders;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.hadoop.yarn.api.ApplicationConstants;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

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
    // Should find the Spark ClassLoader
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
   * Creates a new SparkClassLoader from the given {@link SparkRuntimeContext} without the ability to create
   * {@link SparkExecutionContext}. It is used in {@link Spark#beforeSubmit(SparkClientContext)} and
   * {@link Spark#onFinish(boolean, SparkClientContext)} methods.
   */
  public SparkClassLoader(SparkRuntimeContext runtimeContext) {
    this(runtimeContext, null);
  }

  /**
   * Creates a new SparkClassLoader with the given {@link SparkRuntimeContext}.
   */
  public SparkClassLoader(SparkRuntimeContext runtimeContext, @Nullable SparkExecutionContextFactory contextFactory) {
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
    if (contextFactory == null) {
      // This shouldn't happen, but to safeguard
      throw new IllegalStateException("Creation of SparkExecutionContext is not allowed in the current context.");
    }
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
   * Creates a {@link SparkExecutionContextFactory} to be used in distributed mode. This method only gets called
   * from the driver node if running in yarn-cluster mode.
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

        // Try to determine the hostname from the NM_HOST environment variable, which is set by NM
        String host = System.getenv(ApplicationConstants.Environment.NM_HOST.key());
        if (host == null) {
          // If it is missing, use the current hostname
          try {
            host = InetAddress.getLocalHost().getCanonicalHostName();
          } catch (UnknownHostException e) {
            // Nothing much we can do. Just throw exception since
            // we need the hostname to start the SparkTransactionService
            throw Throwables.propagate(e);
          }
        }
        return new DefaultSparkExecutionContext(runtimeContext, localizeResources, host);
      }
    };
  }
}

