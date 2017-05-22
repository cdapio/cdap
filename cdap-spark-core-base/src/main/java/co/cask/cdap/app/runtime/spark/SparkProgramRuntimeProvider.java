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
import co.cask.cdap.app.runtime.spark.classloader.SparkRunnerClassLoader;
import co.cask.cdap.app.runtime.spark.distributed.DistributedSparkProgramRunner;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.ProvisionException;
import com.google.inject.spi.InstanceBinding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.net.URL;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * A {@link ProgramRuntimeProvider} that provides runtime system support for {@link ProgramType#SPARK} program.
 * This class shouldn't have dependency on Spark classes.
 */
public abstract class SparkProgramRuntimeProvider implements ProgramRuntimeProvider {

  private static final Logger LOG = LoggerFactory.getLogger(SparkProgramRuntimeProvider.class);

  private final SparkCompat providerSparkCompat;
  private final boolean filterScalaClasses;
  private ClassLoader distributedRunnerClassLoader;
  private URL[] classLoaderUrls;

  protected SparkProgramRuntimeProvider(SparkCompat providerSparkCompat) {
    this(providerSparkCompat, false);
  }

  /**
   * Constructor used by SparkTwillRunnable only in distributed mode to enable scala class filtering.
   */
  protected SparkProgramRuntimeProvider(SparkCompat providerSparkCompat, boolean filterScalaClasses) {
    this.providerSparkCompat = providerSparkCompat;
    this.filterScalaClasses = filterScalaClasses;
  }

  @Override
  public ProgramRunner createProgramRunner(ProgramType type, Mode mode, Injector injector) {
    Preconditions.checkArgument(type == ProgramType.SPARK, "Unsupported program type %s. Only %s is supported",
                                type, ProgramType.SPARK);

    switch (mode) {
      case LOCAL:
        // Rewrite YarnClient based on config. The LOCAL runner is used in both SDK and distributed mode
        // The actual mode that Spark is running is determined by the cdap.spark.cluster.mode attribute
        // in the hConf
        boolean rewriteYarnClient = injector.getInstance(CConfiguration.class)
                                            .getBoolean(Constants.AppFabric.SPARK_YARN_CLIENT_REWRITE);
        try {
          SparkRunnerClassLoader classLoader = createClassLoader(filterScalaClasses, rewriteYarnClient);
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
        ClassLoader classLoader = getDistributedRunnerClassLoader();
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
    SparkCompat runtimeSparkCompat = SparkCompat.get(cConf);
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

  private synchronized ClassLoader getDistributedRunnerClassLoader() {
    try {
      if (distributedRunnerClassLoader == null) {
        // Never needs to rewrite yarn client in CDAP master, which is the only place using distributed program runner
        distributedRunnerClassLoader = createClassLoader(true, false);
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
        return createInstance(injector, classLoader.loadClass(programRunnerClassName), classLoader);
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
   * @param type the {@link Class} of the instance to create
   * @return a new instance of the given {@link Type}
   */
  private <T> T createInstance(Injector injector, Type type, ClassLoader sparkClassLoader) throws Exception {
    Key<?> typeKey = Key.get(type);

    // If there is an explicit instance binding, return the binded instance directly
    Binding<?> binding = injector.getExistingBinding(typeKey);
    if (binding != null && binding instanceof InstanceBinding) {
      return (T) ((InstanceBinding) binding).getInstance();
    }

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
      throw new ProvisionException(
        "No constructor is annotated with @Inject and there is no default constructor for class " + type.getName(), e);
    }
  }

  /**
   * Returns an array of {@link URL} being used by the {@link ClassLoader} of this {@link Class}.
   */
  private synchronized SparkRunnerClassLoader createClassLoader(boolean filterScalaClasses,
                                                                boolean rewriteYarnClient) throws IOException {
    // Determine if needs to filter Scala classes or not.
    ClassLoader runnerParentClassLoader = filterScalaClasses
      ? new ScalaFilterClassLoader(getClass().getClassLoader()) : getClass().getClassLoader();

    if (classLoaderUrls == null) {
      classLoaderUrls = getSparkClassloaderURLs(getClass().getClassLoader());
    }

    SparkRunnerClassLoader runnerClassLoader = new SparkRunnerClassLoader(classLoaderUrls,
                                                                          runnerParentClassLoader, rewriteYarnClient);

    // CDAP-8087: Due to Scala 2.10 bug in not able to support runtime reflection from multiple threads,
    // we create the runtime mirror from this synchronized method
    // (there is only one instance of SparkProgramRuntimeProvider), and the mirror will be cached by Scala in a
    // WeakHashMap. Any future call to "scala.reflect.runtime.universe.runtimeMirror(classLoader)" will simply returns
    // the cached runtimeMirror from the WeakHashMap. This is a workaround the Scala 2.10 bug SI-6240 to avoid
    // concurrent creation of scala runtimeMirror when the runtimeMirror is needed in Spark yarn/Client.scala and
    // triggered from Workflow fork.

    try {
      // Use reflection to call scala.reflect.runtime.package$.MODULE$.universe.runtimeMirror(classLoader)
      // The scala.reflect.runtime.package$ is the class, which has a public MODULE$ field, which is scala way of
      // doing singleton object.
      // We use reflection to avoid code dependency  on the scala version.
      Object scalaReflect = runnerClassLoader.loadClass("scala.reflect.runtime.package$").getField("MODULE$").get(null);
      Object javaMirrors = scalaReflect.getClass().getMethod("universe").invoke(scalaReflect);
      javaMirrors.getClass().getMethod("runtimeMirror", ClassLoader.class).invoke(javaMirrors, runnerClassLoader);
    } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException e) {
      // The SI-6240 is fixed in Scala 2.11 anyway and older Scala version should have the classes and methods
      // that we try to invoke above.
      LOG.debug("Not able to create scala runtime mirror for SparkRunnerClassLoader. " +
                  "This can happen if there is incompatible Scala API changes with Scala version newer than 2.10. " +
                  "However, the SI-6240 bug is fixed since 2.11, hence the workaround for the the bug is not needed ");
    } catch (Exception e) {
      LOG.warn("Failed to create scala runtime mirror for SparkRunnerClassLoader. " +
                 "Running multiple Spark from the same JVM process might fail due to Scala reflection bug SI-6240.", e);
    }

    return runnerClassLoader;
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
      Iterator<URL> itor = urls.iterator();
      while (itor.hasNext()) {
        URL url = itor.next();
        if (url.getPath().contains("org.scala-lang")) {
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
   * A ClassLoader that filter out scala class.
   */
  private static final class ScalaFilterClassLoader extends ClassLoader {

    ScalaFilterClassLoader(ClassLoader parent) {
      super(new FilterClassLoader(parent, new FilterClassLoader.Filter() {
        @Override
        public boolean acceptResource(String resource) {
          return !resource.startsWith("scala/") && !"scala.class".equals(resource);
        }

        @Override
        public boolean acceptPackage(String packageName) {
          return !packageName.startsWith("scala/");
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
      String baseClasspath = ClassLoaders.getClassPathURL(name, resource).getPath();
      String jarName = baseClasspath.substring(baseClasspath.lastIndexOf('/') + 1, baseClasspath.length());
      return jarName.startsWith("org.scala-lang") ? null : resource;
    }
  }
}
