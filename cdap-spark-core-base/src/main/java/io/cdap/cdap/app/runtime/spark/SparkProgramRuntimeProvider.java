/*
 * Copyright © 2016-2021 Cask Data, Inc.
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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Binding;
import com.google.inject.BindingAnnotation;
import com.google.inject.ConfigurationException;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.ProvisionException;
import com.google.inject.spi.InstanceBinding;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramRuntimeProvider;
import io.cdap.cdap.app.runtime.spark.classloader.SparkRunnerClassLoader;
import io.cdap.cdap.app.runtime.spark.distributed.DistributedSparkProgramRunner;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.internal.app.spark.SparkCompatReader;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.runtime.spi.SparkCompat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * A {@link ProgramRuntimeProvider} that provides runtime system support for {@link ProgramType#SPARK} program.
 * This class shouldn't have dependency on Spark classes.
 */
public abstract class SparkProgramRuntimeProvider implements ProgramRuntimeProvider {

  private static final Logger LOG = LoggerFactory.getLogger(SparkProgramRuntimeProvider.class);

  static {
    try {
      // Load the shutdown manager to have the ShutdownHookManager static initialization block execute
      // This is to avoid having the shutdown hook thread holding reference to SparkRunnerClassLoader
      // when it is being used by Spark.
      Class.forName("org.apache.hadoop.util.ShutdownHookManager");
    } catch (ClassNotFoundException e) {
      // It's ok if the shutdown hook manager from Hadoop is not there
    }
  }

  private final SparkCompat providerSparkCompat;
  private final boolean filterScalaClasses;
  private ClassLoader distributedRunnerClassLoader;
  private URL[] classLoaderUrls;

  protected SparkProgramRuntimeProvider(SparkCompat providerSparkCompat) {
    this.providerSparkCompat = providerSparkCompat;
    this.filterScalaClasses = Boolean.parseBoolean(System.getenv(SparkPackageUtils.SPARK_YARN_MODE));
  }

  @Override
  public ClassLoader createProgramClassLoader(CConfiguration cConf, ProgramType type) {
    Preconditions.checkArgument(type == ProgramType.SPARK, "Unsupported program type %s. Only %s is supported",
                                type, ProgramType.SPARK);
    boolean rewriteCheckpointTempFileName =
      cConf.getBoolean(SparkRuntimeUtils.SPARK_STREAMING_CHECKPOINT_REWRITE_ENABLED);
    boolean rewriteYarnClient = cConf.getBoolean(Constants.AppFabric.SPARK_YARN_CLIENT_REWRITE);

    try {
      ClassLoader sparkRunnerClassLoader = sparkRunnerClassLoader = createClassLoader(filterScalaClasses,
                                                                                      rewriteYarnClient,
                                                                                      rewriteCheckpointTempFileName);

      // SparkResourceFilter must be instantiated using the above classloader as it has the
      // org.apache.spark.streaming.StreamingContext class, otherwise it will cause NoClassDefFoundError at runtime
      // because the parent classloader does not have Spark resources loaded.
      FilterClassLoader.Filter sparkClassLoaderFilter = (FilterClassLoader.Filter) sparkRunnerClassLoader
        .loadClass(SparkResourceFilter.class.getName()).newInstance();
      return new FilterClassLoader(sparkRunnerClassLoader, sparkClassLoaderFilter);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("Failed to create SparkResourceFilter class", e);
    }
  }

  @Override
  public ProgramRunner createProgramRunner(ProgramType type, Mode mode, Injector injector) {
    Preconditions.checkArgument(type == ProgramType.SPARK, "Unsupported program type %s. Only %s is supported",
                                type, ProgramType.SPARK);

    CConfiguration conf = injector.getInstance(CConfiguration.class);

    boolean rewriteCheckpointTempFileName =
      conf.getBoolean(SparkRuntimeUtils.SPARK_STREAMING_CHECKPOINT_REWRITE_ENABLED);

    switch (mode) {
      case LOCAL:
        // Rewrite YarnClient based on config. The LOCAL runner is used in both SDK and distributed mode
        // The actual mode that Spark is running is determined by the cdap.spark.cluster.mode attribute
        // in the hConf
        boolean rewriteYarnClient = conf.getBoolean(Constants.AppFabric.SPARK_YARN_CLIENT_REWRITE);
        try {
          SparkRunnerClassLoader classLoader = createClassLoader(filterScalaClasses, rewriteYarnClient,
                                                                 rewriteCheckpointTempFileName);
          try {
            // Closing of the SparkRunnerClassLoader is done by the SparkProgramRunner when the program execution
            // finished.
            // The current CDAP call run right after it get a ProgramRunner and never reuse a ProgramRunner.
            // TODO: CDAP-5506 to refactor the program runtime architecture to remove the need of this assumption
            return createSparkProgramRunner(createRunnerInjector(injector, classLoader),
                                            SparkProgramRunner.class.getName(), classLoader);
          } catch (Throwable t) {
            // If there is any exception, close the classloader
            Closeables.closeQuietly(classLoader);
            throw t;
          }
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      case DISTRIBUTED:
        // The distributed program runner is only used by the CDAP master to launch the twill container
        // hence it doesn't need to do any class rewrite.
        // We only create the SparkRunnerClassLoader once and keep reusing it since in the CDAP master, there is
        // no SparkContext being created, hence no need to provide runtime isolation.
        // This also limits the amount of permgen usage to be constant in the CDAP master regardless of how
        // many Spark programs are running. We never need to close the SparkRunnerClassLoader until process shutdown.
        ClassLoader classLoader = getDistributedRunnerClassLoader(rewriteCheckpointTempFileName);
        return createSparkProgramRunner(createRunnerInjector(injector, classLoader),
                                        DistributedSparkProgramRunner.class.getName(),
                                        classLoader);
      default:
        throw new IllegalArgumentException("Unsupported Spark execution mode " + mode);
    }
  }

  @Override
  public boolean isSupported(ProgramType programType, CConfiguration cConf) {
    // TODO: Need to check if it actually has the corresponding spark version available
    SparkCompat runtimeSparkCompat = SparkCompatReader.get(cConf);
    if (runtimeSparkCompat == providerSparkCompat) {
      LOG.debug("using sparkCompat {}", providerSparkCompat);
      return true;
    }
    return false;
  }

  /**
   * Creates a guice {@link Injector} for instantiating program runner.
   *
   * @param injector the parent injector
   * @param runnerClassLoader the classloader for the program runner class
   * @return a new injector for injection for program runner.
   */
  private Injector createRunnerInjector(Injector injector, final ClassLoader runnerClassLoader) {
    return injector.createChildInjector(new AbstractModule() {
      @Override
      protected void configure() {
        // Add a binding for SparkCompat enum
        // The binding needs to be on the enum class loaded by the runner classloader
        try {
          Class<? extends Enum> type = (Class<? extends Enum>) runnerClassLoader.loadClass(SparkCompat.class.getName());
          bindEnum(binder(), type, providerSparkCompat.name());
        } catch (ClassNotFoundException e) {
          // This shouldn't happen
          throw Throwables.propagate(e);
        }
      }

      private <T extends Enum<T>> void bindEnum(Binder binder, Class<T> enumType, String name) {
        // This workaround the enum generic
        binder.bind(enumType).toInstance(Enum.valueOf(enumType, name));
      }
    });
  }

  private synchronized ClassLoader getDistributedRunnerClassLoader(boolean rewriteCheckpointTempFileName) {
    try {
      if (distributedRunnerClassLoader == null) {
        // Never needs to rewrite yarn client in CDAP master, which is the only place using distributed program runner
        distributedRunnerClassLoader = createClassLoader(true, false, rewriteCheckpointTempFileName);
      }
      return distributedRunnerClassLoader;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Creates a {@link ProgramRunner} that execute Spark program from the given {@link Injector}.
   */
  private ProgramRunner createSparkProgramRunner(Injector injector,
                                                 String programRunnerClassName,
                                                 ClassLoader classLoader) {
    try {
      ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(classLoader);
      try {
        return createInstance(injector, Key.get(classLoader.loadClass(programRunnerClassName)), classLoader);
      } finally {
        ClassLoaders.setContextClassLoader(oldClassLoader);
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
   * @param key the guice binding {@link Key} for acquiring an instance that binded to the given key.
   * @return a new instance of the given {@link Type}
   */
  private <T> T createInstance(Injector injector, Key<?> key, ClassLoader sparkClassLoader) throws Exception {
    // If there is an explicit instance binding, return the binded instance directly
    Binding<?> binding = injector.getExistingBinding(key);
    if (binding != null && binding instanceof InstanceBinding) {
      return (T) ((InstanceBinding) binding).getInstance();
    }

    @SuppressWarnings("unchecked")
    Class<T> rawType = (Class<T>) key.getTypeLiteral().getRawType();

    Constructor<T> constructor = findInjectableConstructor(rawType);
    constructor.setAccessible(true);

    // Acquire the instances for each parameter for the constructor
    Type[] paramTypes = constructor.getGenericParameterTypes();
    Annotation[][] paramAnnotations = constructor.getParameterAnnotations();
    Object[] args = new Object[paramTypes.length];

    for (int i = 0; i < paramTypes.length; i++) {
      Type paramType = paramTypes[i];

      // Find if there is an annotation which itself is annotated with @BindingAnnotation.
      // If there is one, and it can only have one according to Guice,
      // use it to construct the binding key for the parmater.
      // Otherwise, just use the parameter type.
      Optional<Annotation> bindingAnnotation = Arrays.stream(paramAnnotations[i])
        .filter(paramAnnotation ->
                  Arrays.stream(paramAnnotation.annotationType().getAnnotations())
                    .map(Annotation::annotationType)
                    .anyMatch(BindingAnnotation.class::equals))
        .findFirst();

      Key<?> paramTypeKey;
      if (bindingAnnotation.isPresent()) {
        paramTypeKey = Key.get(paramType, bindingAnnotation.get());
      } else {
        paramTypeKey = Key.get(paramType);
      }

      try {
        // If the classloader of the parameter is the same as the Spark ClassLoader, we need to create the
        // instance manually instead of getting through the Guice Injector to avoid ClassLoader leakage
        if (paramTypeKey.getTypeLiteral().getRawType().getClassLoader() == sparkClassLoader) {
          args[i] = createInstance(injector, paramTypeKey, sparkClassLoader);
        } else {
          args[i] = injector.getInstance(paramTypeKey);
        }
      } catch (ConfigurationException e) {
        // Wrap the Guice ConfigurationException with information on what class is getting bound to allow for debugging.
        throw new RuntimeException(String.format("Failed to bind paramTypeKey '%s' for paramType '%s' in key '%s'",
                                                 paramTypeKey, paramType, key), e);
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
      throw new ProvisionException(
        "No constructor is annotated with @Inject and there is no default constructor for class " + type.getName(), e);
    }
  }

  /**
   * Returns an array of {@link URL} being used by the {@link ClassLoader} of this {@link Class}.
   */
  private synchronized SparkRunnerClassLoader createClassLoader(boolean filterScalaClasses,
                                                                boolean rewriteYarnClient,
                                                                boolean rewriteCheckpointTempName) throws IOException {
    // Determine if needs to filter Scala classes or not.
    FilterClassLoader filteredBaseParent = new FilterClassLoader(getClass().getClassLoader(), createClassFilter());
    ClassLoader runnerParentClassLoader = filterScalaClasses
      ? new ScalaFilterClassLoader(filteredBaseParent) : filteredBaseParent;

    if (classLoaderUrls == null) {
      classLoaderUrls = getSparkClassloaderURLs(getClass().getClassLoader());
    }

    return new SparkRunnerClassLoader(classLoaderUrls,
                                      runnerParentClassLoader,
                                      rewriteYarnClient,
                                      rewriteCheckpointTempName);
  }

  /**
   * Returns an array of URLs to be used for creation of classloader for Spark, based on the urls used by the
   * given {@link ClassLoader}.
   */
  private URL[] getSparkClassloaderURLs(ClassLoader classLoader) throws IOException {
    List<URL> urls = ClassLoaders.getClassLoaderURLs(classLoader, new LinkedList<URL>());

    // If Spark classes are not available in the given ClassLoader, try to locate the Spark framework
    // This class cannot have dependency on Spark directly, hence using the class resource to discover if SparkContext
    // is there
    if (classLoader.getResource("org/apache/spark/SparkContext.class") == null) {
      // The scala from the Spark library should replace the one from the parent classloader
      // CDH also packages spark-yarn-shuffle.jar linked to spark-<version>-yarn-shuffle.jar in yarn's lib dir
      // Also filter out all jackson libraries that comes from the parent
      Iterator<URL> itor = urls.iterator();
      while (itor.hasNext()) {
        URL url = itor.next();
        String filename = Paths.get(url.getPath()).getFileName().toString();
        if (filename.startsWith("org.scala-lang") || filename.startsWith("spark-") || filename.contains("jackson-")) {
          itor.remove();
        }
      }

      for (File file : SparkPackageUtils.getLocalSparkLibrary(providerSparkCompat)) {
        urls.add(file.toURI().toURL());
      }
    }

    return urls.toArray(new URL[urls.size()]);
  }

  /**
   * Returns {@code true} if the given string is equals to the equals string, or if the given string is
   * starts with the given equal string concatenated with the given suffix.
   */
  private static boolean equalsOrStartWith(String str, String equalStr, char startWithSuffix) {
    return str.equals(equalStr) || str.startsWith(equalStr + startWithSuffix);
  }

  /**
   * Creates a {@link FilterClassLoader.Filter} for shielding out unwanted classes that comes from the CDAP
   * main classloader. It includes classes that are known to cause conflicts with Spark.
   */
  private FilterClassLoader.Filter createClassFilter() {
    return new FilterClassLoader.Filter() {
      @Override
      public boolean acceptResource(String resource) {
        return !equalsOrStartWith(resource, "com/fasterxml/jackson", '/');
      }

      @Override
      public boolean acceptPackage(String packageName) {
        return !equalsOrStartWith(packageName, "com.fasterxml.jackson", '.');
      }
    };
  }

  /**
   * A ClassLoader that filter out scala class.
   */
  private static final class ScalaFilterClassLoader extends ClassLoader {

    ScalaFilterClassLoader(ClassLoader parent) {
      super(new FilterClassLoader(parent, new FilterClassLoader.Filter() {
        @Override
        public boolean acceptResource(String resource) {
          return !equalsOrStartWith(resource, "org/apache/spark", '/')
            && !equalsOrStartWith(resource, "org/spark-project", '/')
            && !equalsOrStartWith(resource, "scala", '/')
            && !"scala/class".equals(resource);
        }

        @Override
        public boolean acceptPackage(String packageName) {
          return !equalsOrStartWith(packageName, "org.apache.spark", '.')
            && !equalsOrStartWith(packageName, "org.spark-project", '.')
            && !equalsOrStartWith(packageName, "scala", '.');
        }
      }));
    }

    @Override
    public URL getResource(String name) {
      URL resource = super.getResource(name);
      if (resource == null) {
        return null;
      }
      // resource = jar:file:/path/to/cdap/lib/org.scala-lang.scala-library-2.10.4.jar!/library.properties
      // baseClasspath = /path/to/cdap/lib/org.scala-lang.scala-library-2.10.4.jar

      // ignoring jrt scheme for java11 support
      if (resource.getProtocol().equals("jrt")) {
        return null;
      }
      String baseClasspath = ClassLoaders.getClassPathURL(name, resource).getPath();
      String jarName = baseClasspath.substring(baseClasspath.lastIndexOf('/') + 1, baseClasspath.length());
      return jarName.startsWith("org.scala-lang") ? null : resource;
    }
  }
}
