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

package co.cask.cdap.app.runtime.spark.distributed;

import co.cask.cdap.app.runtime.spark.SparkRuntimeContextProvider;
import co.cask.cdap.app.runtime.spark.SparkRuntimeUtils;
import co.cask.cdap.app.runtime.spark.classloader.SparkContainerClassLoader;
import co.cask.cdap.app.runtime.spark.python.SparkPythonUtil;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.common.logging.StandardOutErrorRedirector;
import co.cask.cdap.common.logging.common.UncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * This class launches Spark YARN containers with classes loaded through the {@link SparkContainerClassLoader}.
 */
public final class SparkContainerLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(SparkContainerLauncher.class);
  private static final FilterClassLoader.Filter KAFKA_FILTER = new FilterClassLoader.Filter() {
    @Override
    public boolean acceptResource(String resource) {
      return resource.startsWith("kafka/");
    }

    @Override
    public boolean acceptPackage(String packageName) {
      return packageName.equals("kafka") || packageName.startsWith("kafka.");
    }
  };

  /**
   * Launches the given main class. The main class will be loaded through the {@link SparkContainerClassLoader}.
   *
   * @param mainClassName the main class to launch
   * @param args arguments for the main class
   */
  @SuppressWarnings("unused")
  public static void launch(String mainClassName, String[] args) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler());
    ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
    Set<URL> urls = ClassLoaders.getClassLoaderURLs(systemClassLoader, new LinkedHashSet<URL>());

    // Remove the URL that contains the given main classname to avoid infinite recursion.
    // This is needed because we generate a class with the same main classname in order to intercept the main()
    // method call from the container launch script.
    urls.remove(getURLByClass(systemClassLoader, mainClassName));

    // Remove the first scala from the set of classpath. This ensure the one from Spark is used for spark
    URL scalaURL = getURLByClass(systemClassLoader, "scala.language");
    Enumeration<URL> resources = systemClassLoader.getResources("scala/language.class");
    // Only remove the scala if there are more than one in the classpath
    int count = 0;
    while (resources.hasMoreElements()) {
      resources.nextElement();
      count++;
    }
    if (count > 1) {
      urls.remove(scalaURL);
    }

    // First create a FilterClassLoader that only loads JVM and kafka classes from the system classloader
    // This is to isolate the scala library from children
    ClassLoader parentClassLoader = new FilterClassLoader(systemClassLoader, KAFKA_FILTER);

    // Creates the SparkRunnerClassLoader for class rewriting and it will be used for the rest of the execution.
    // Use the extension classloader as the parent instead of the system classloader because
    // Spark classes are in the system classloader which we want to rewrite.
    ClassLoader classLoader = new SparkContainerClassLoader(urls.toArray(new URL[urls.size()]), parentClassLoader);

    // Sets the context classloader and launch the actual Spark main class.
    Thread.currentThread().setContextClassLoader(classLoader);

    // Install the JUL to SLF4J Bridge
    try {
      classLoader.loadClass(SLF4JBridgeHandler.class.getName())
        .getDeclaredMethod("install")
        .invoke(null);
    } catch (Exception e) {
      // Log the error and continue
      LOG.warn("Failed to invoke SLF4JBridgeHandler.install() required for jul-to-slf4j bridge", e);
    }

    try {
      // Get the SparkRuntimeContext to initialize all necessary services and logging context
      // Need to do it using the SparkRunnerClassLoader through reflection.
      classLoader.loadClass(SparkRuntimeContextProvider.class.getName()).getMethod("get").invoke(null);

      // For non-PySpark, do the logs redirection. Otherwise the log redirect is done
      // in the PythonRunner/PythonWorkerFactory via SparkClassRewriter.
      if (!isPySpark()) {
        // Invoke StandardOutErrorRedirector.redirectToLogger()
        classLoader.loadClass(StandardOutErrorRedirector.class.getName())
          .getDeclaredMethod("redirectToLogger", String.class)
          .invoke(null, mainClassName);
      }

      // Force setting the system property CDAP_LOG_DIR to <LOG_DIR>. This is to workaround bug in Spark 1.2
      // that it passes executor environment via command line properties, which get resolved by yarn launcher,
      // which causes executor logs attempt to write to driver log directory
      if (System.getProperty("spark.executorEnv.CDAP_LOG_DIR") != null) {
        System.setProperty("spark.executorEnv.CDAP_LOG_DIR", "<LOG_DIR>");
      }

      // Optionally starts Py4j Gateway server in the executor container
      Runnable stopGatewayServer = startGatewayServerIfNeeded(classLoader);
      try {
        LOG.info("Launch main class {}.main({})", mainClassName, Arrays.toString(args));
        classLoader.loadClass(mainClassName).getMethod("main", String[].class).invoke(null, new Object[]{args});
        LOG.info("Main method returned {}", mainClassName);
      } finally {
        stopGatewayServer.run();
      }
    } catch (Throwable t) {
      // LOG the exception since this exception will be propagated back to JVM
      // and kill the main thread (hence the JVM process).
      // If we don't log it here as ERROR, it will be logged by UncaughtExceptionHandler as DEBUG level
      LOG.error("Exception raised when calling {}.main(String[]) method", mainClassName, t);
      throw t;
    }
  }

  /**
   * Gets the URL that has the given class loaded from the given ClassLoader.
   */
  private static URL getURLByClass(ClassLoader classLoader, String className) {
    URL resource = classLoader.getResource(className.replace('.', '/') + ".class");
    if (resource == null) {
      throw new IllegalStateException("Failed to find .class file resource for class " + className);
    }
    return ClassLoaders.getClassPathURL(className, resource);
  }

  /**
   * Starts Py4j gateway server if in executor container and running PySpark.
   *
   * @param classLoader the classloader to use for loading classes
   * @return a {@link Runnable} that calling the {@link Runnable#run()} method will stop the server.
   */
  private static Runnable startGatewayServerIfNeeded(ClassLoader classLoader) {
    Runnable noopRunnable = new Runnable() {
      @Override
      public void run() {
        // no-op
      }
    };

    // If we are not running PySpark or
    // if this process is the AM, no need to start gateway server
    // The spark execution service uri is always set for the driver (AM) process
    if (!isPySpark() || System.getenv(SparkRuntimeUtils.CDAP_SPARK_EXECUTION_SERVICE_URI) != null) {
      return noopRunnable;
    }

    // Otherwise start the gateway server using reflection. Also write the port number to a local file
    Path portFile = Paths.get("cdap.py4j.gateway.port.txt");
    try {
      final Object server = classLoader.loadClass(SparkPythonUtil.class.getName())
        .getMethod("startPy4jGateway", Path.class).invoke(null, portFile);

      LOG.info("Py4j GatewayServer started, listening at port {}",
               new String(Files.readAllBytes(portFile), StandardCharsets.UTF_8));

      return new Runnable() {
        @Override
        public void run() {
          try {
            server.getClass().getMethod("shutdown").invoke(server);
          } catch (Exception e) {
            LOG.warn("Failed to shutdown Py4j GatewayServer", e);
          }
        }
      };
    } catch (Exception e) {
      LOG.warn("Failed to start Py4j GatewayServer. No CDAP functionality will be available in executor", e);
      return noopRunnable;
    }
  }

  private static boolean isPySpark() {
    return System.getenv("PYTHONPATH") != null;
  }
}
