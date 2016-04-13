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
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.internal.app.runtime.spark.SparkUtils;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.ProvisionException;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
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

  private static final String SPARK_PROGRAM_RUNNER_CLASS_NAME = SparkProgramRunner.class.getName();
  private URL[] classLoaderUrls;

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
    try {
      CConfiguration cConf = injector.getInstance(CConfiguration.class);
      final SparkRunnerClassLoader classLoader = new SparkRunnerClassLoader(getClassLoaderURLs(cConf),
                                                                            getClass().getClassLoader());
      try {
        ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(classLoader);
        try {
          // Closing of the SparkRunnerClassLoader is done by the SparkProgramRunner when the program execution finished
          // The current CDAP call run right after it get a ProgramRunner and never reuse a ProgramRunner.
          // TODO: CDAP-5506 to refactor the program runtime architecture to remove the need of this assumption
          return createInstance(injector, classLoader.loadClass(SPARK_PROGRAM_RUNNER_CLASS_NAME), classLoader);
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

  /**
   * Create a new instance of the given {@link Type} from the given {@link Injector}. This method
   * is doing Guice injection manually through the @Inject constructor to avoid ClassLoader leakage
   * due to the just-in-time binding map inside the Guice Injector that holds a strong reference to the type,
   * hence the ClassLoader of that type
   *
   * @param injector The Guice Injector for acquiring CDAP system instances
   * @param type the {@link Class} of the instance to create
   * @return a new instance of the given {@link Type}
   */
  private <T> T createInstance(Injector injector, Type type, ClassLoader sparkClassLoader) throws Exception {
    Key<?> typeKey = Key.get(type);
    @SuppressWarnings("unchecked")
    Class<T> rawType = (Class<T>) typeKey.getTypeLiteral().getRawType();

    Constructor<T> constructor = findInjectableConstructor(rawType);
    constructor.setAccessible(true);

    // Acquire the instances for each parameter for the constructor
    Type[] paramTypes = constructor.getGenericParameterTypes();
    Object[] args = new Object[paramTypes.length];
    int i = 0;
    for (Type paramType : paramTypes) {
      Key<?> paramTypeKey = Key.get(paramType);

      // If the classloader of the parameter is the same as the Spark ClassLoader, we need to create the
      // instance manually instead of getting through the Guice Injector to avoid ClassLoader leakage
      if (paramTypeKey.getTypeLiteral().getRawType().getClassLoader() == sparkClassLoader) {
        args[i++] = createInstance(injector, paramType, sparkClassLoader);
      } else {
        args[i++] = injector.getInstance(paramTypeKey);
      }
    }
    return constructor.newInstance(args);
  }

  /**
   * Finds the constructor of the given type that is suitable for Guice injection. If the given type has
   * a constructor annotated with {@link Inject}, then it will be returned. Otherwise, the default constructor
   * will be returned.
   *
   * @throws ProvisionException if failed to locate a constructor for the injection
   */
  @SuppressWarnings("unchecked")
  private <T> Constructor<T> findInjectableConstructor(Class<T> type) throws ProvisionException {
    for (Constructor<?> constructor : type.getDeclaredConstructors()) {
      // Find the @Inject constructor
      if (constructor.isAnnotationPresent(Inject.class)) {
        return (Constructor<T>) constructor;
      }
    }

    // If no @Inject constructor, use the default constructor
    try {
      return type.getDeclaredConstructor();
    } catch (NoSuchMethodException e) {
      throw new ProvisionException("No constructor is annotated with @Inject and there is no default constructor", e);
    }
  }

  private synchronized URL[] getClassLoaderURLs(CConfiguration cConf) throws IOException {
    if (classLoaderUrls != null) {
      return classLoaderUrls;
    }
    classLoaderUrls = getClassLoaderURLs(getClass().getClassLoader(), cConf);
    return classLoaderUrls;
  }

  /**
   * Returns an array of {@link URL} that the given ClassLoader uses, including all URLs used by the parent of the
   * given ClassLoader. It will also includes the Spark aseembly jar if Spark classes are not loadable from
   * the given ClassLoader.
   */
  private URL[] getClassLoaderURLs(ClassLoader classLoader, CConfiguration cConf) throws IOException {
    List<URL> urls = getClassLoaderURLs(classLoader, new ArrayList<URL>());

    // If Spark classes are not available in the given ClassLoader, try to locate the Spark assembly jar
    // This class cannot have dependency on Spark directly, hence using the class resource to discover if SparkContext
    // is there
    if (classLoader.getResource("org/apache/spark/SparkContext.class") == null) {
      urls.add(SparkUtils.getRewrittenSparkAssemblyJar(cConf).toURI().toURL());
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
