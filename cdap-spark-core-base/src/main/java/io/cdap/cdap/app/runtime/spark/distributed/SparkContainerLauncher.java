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

package io.cdap.cdap.app.runtime.spark.distributed;

import com.google.common.io.Closeables;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContextProvider;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeUtils;
import io.cdap.cdap.app.runtime.spark.classloader.SparkContainerClassLoader;
import io.cdap.cdap.app.runtime.spark.python.SparkPythonUtil;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.lang.ClassPathResources;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.common.logging.StandardOutErrorRedirector;
import io.cdap.cdap.common.logging.common.UncaughtExceptionHandler;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * This class launches Spark YARN containers with classes loaded through the
 * {@link SparkContainerClassLoader}.
 */
public final class SparkContainerLauncher {

  // This logger is only for logging for this class.
  private static final Logger LOG = LoggerFactory.getLogger(SparkContainerLauncher.class);
  private static final String SPARK_CONF_CLASS_NAME = "org.apache.spark.SparkConf";
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
   * Main method is used as the entrypoint to launch classes in Kubernetes images. The first
   * argument should be the name of the class to delegate to, which must have a main method. Every
   * other argument will be passed into the delegate main method.
   */
  public static void main(String[] args) throws Exception {
    launch(args[0], args.length > 1 ? Arrays.copyOfRange(args, 1, args.length) : new String[0],
        false, "k8s");
  }

  /**
   * Launches the given main class. The main class will be loaded through the
   * {@link SparkContainerClassLoader}.
   *
   * @param mainClassName the main class to launch
   * @param args arguments for the main class
   */
  public static void launch(String mainClassName, String[] args) throws Exception {
    launch(mainClassName, args, true, null);
  }

  /**
   * Launches the given main class. The main class will be loaded through the
   * {@link SparkContainerClassLoader}.
   *
   * @param mainClassName the main class to launch
   * @param args arguments for the main class
   * @param removeMainClass whether to remove the jar for the main class from the classloader
   * @param masterEnvName name of the MasterEnvironment used to submit the Spark job. This will
   *     be used to setup bindings for service discovery and other CDAP capabilities. If null, the
   *     default Hadoop implementations will be used.
   */
  public static void launch(String mainClassName, String[] args, boolean removeMainClass,
      @Nullable String masterEnvName) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler());

    Set<URL> urls = new LinkedHashSet<>();

    for (String path : System.getProperty("java.class.path").split(File.pathSeparator)) {
      urls.add(Paths.get(path).toRealPath().toUri().toURL());
    }

    ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();

    // Remove the URL that contains the given main classname to avoid infinite recursion.
    // This is needed because we generate a class with the same main classname in order to intercept the main()
    // method call from the container launch script.
    if (removeMainClass) {
      URL urlByClass = getURLByClass(systemClassLoader, mainClassName);
      if (!urls.remove(urlByClass)) {
        urls.forEach(url -> LOG.info("URL: {}", url));
        throw new Exception("Failed to remove url " + urlByClass + " for class " + mainClassName);
      }
    }

    // Remove the first scala from the set of classpath. This ensure the one from Spark is used for spark
    removeNonSparkJar(systemClassLoader, "scala.language", urls);
    // Remove the first jar containing LZBlockInputStream from the set of classpath.
    // The one from Kafka is not compatible with Spark
    removeNonSparkJar(systemClassLoader, "net.jpountz.lz4.LZ4BlockInputStream", urls);

    // First create a FilterClassLoader that only loads JVM and kafka classes from the system classloader
    // This is to isolate the scala library from children
    ClassLoader parentClassLoader = new FilterClassLoader(systemClassLoader, KAFKA_FILTER);

    boolean rewriteCheckpointTempFileName = Boolean.parseBoolean(
        System.getProperty(SparkRuntimeUtils.STREAMING_CHECKPOINT_REWRITE_ENABLED, "false"));

    // Creates the SparkRunnerClassLoader for class rewriting and it will be used for the rest of the execution.
    // Use the extension classloader as the parent instead of the system classloader because
    // Spark classes are in the system classloader which we want to rewrite.
    ClassLoader classLoader = new SparkContainerClassLoader(urls.toArray(new URL[0]),
        parentClassLoader,
        rewriteCheckpointTempFileName);

    // Sets the context classloader and launch the actual Spark main class.
    Thread.currentThread().setContextClassLoader(classLoader);

    // Create SLF4J logger from the context classloader. It has to be created from that classloader in order
    // for logs in this class to be in the same context as the one used in Spark.
    Object logger = createLogger(classLoader);

    // Install the JUL to SLF4J Bridge
    try {
      classLoader.loadClass(SLF4JBridgeHandler.class.getName())
          .getDeclaredMethod("install")
          .invoke(null);
    } catch (Exception e) {
      // Log the error and continue
      log(logger, "warn",
          "Failed to invoke SLF4JBridgeHandler.install() required for jul-to-slf4j bridge", e);
    }

    // Get the SparkRuntimeContext to initialize all necessary services and logging context
    // Need to do it using the SparkRunnerClassLoader through reflection.
    Class<?> sparkRuntimeContextProviderClass = classLoader.loadClass(
        SparkRuntimeContextProvider.class.getName());
    if (masterEnvName != null) {
      sparkRuntimeContextProviderClass.getMethod("setMasterEnvName", String.class)
          .invoke(null, masterEnvName);
    }
    Object sparkRuntimeContext = sparkRuntimeContextProviderClass.getMethod("get").invoke(null);

    if (sparkRuntimeContext instanceof Closeable) {
      System.setSecurityManager(new SparkRuntimeSecurityManager((Closeable) sparkRuntimeContext));
    }

    try {
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
      Runnable stopGatewayServer = startGatewayServerIfNeeded(classLoader, logger);
      try {
        log(logger, "info", "Launch main class {}.main({})", mainClassName, Arrays.toString(args));
        classLoader.loadClass(mainClassName).getMethod("main", String[].class)
            .invoke(null, new Object[]{args});
        log(logger, "info", "Main method returned {}", mainClassName);
      } finally {
        stopGatewayServer.run();
      }
    } catch (Throwable t) {
      // LOG the exception since this exception will be propagated back to JVM
      // and kill the main thread (hence the JVM process).
      // If we don't log it here as ERROR, it will be logged by UncaughtExceptionHandler as DEBUG level
      log(logger, "error", "Exception raised when calling {}.main(String[]) method",
          mainClassName, t);
      throw t;
    } finally {
      if (sparkRuntimeContext instanceof Closeable) {
        Closeables.closeQuietly((Closeable) sparkRuntimeContext);
      }
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
  private static Runnable startGatewayServerIfNeeded(ClassLoader classLoader, Object logger) {
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
    try {
      final Object server = classLoader.loadClass(SparkPythonUtil.class.getName())
          .getMethod("startPy4jGateway", Path.class)
          .invoke(null, Paths.get(System.getProperty("user.dir")));

      return new Runnable() {
        @Override
        public void run() {
          try {
            server.getClass().getMethod("shutdown").invoke(server);
          } catch (Exception e) {
            log(logger, "warn", "Failed to shutdown Py4j GatewayServer", e);
          }
        }
      };
    } catch (Exception e) {
      log(logger, "warn",
          "Failed to start Py4j GatewayServer. No CDAP functionality will be available in executor",
          e);
      return noopRunnable;
    }
  }

  private static boolean isPySpark() {
    return System.getenv("PYTHONPATH") != null;
  }

  /**
   * Removes extra classpath containing the given class that is not from the spark library loaded
   * from the given {@link ClassLoader} if there are multiple files containing the given class.
   *
   * @param classLoader the {@link ClassLoader} for finding the jar containing the given class
   * @param className the class to look for
   * @param urls a {@link Set} of {@link URL} for the removal jar file
   * @throws IOException if failed to lookup the class location
   */
  private static void removeNonSparkJar(ClassLoader classLoader,
      String className, Set<URL> urls) throws IOException, URISyntaxException {
    URL sparkConfURL = getURLByClass(classLoader, SPARK_CONF_CLASS_NAME);
    List<URL> classURLs = Collections.list(
        classLoader.getResources(className.replace('.', '/') + ".class"));

    if (classURLs.size() <= 1) {
      return;
    }

    // Remove URLs that are not from Spark
    URL sparkLibURL = getParentURL(sparkConfURL);
    for (URL classURL : classURLs) {
      URL classPathURL = ClassLoaders.getClassPathURL(className, classURL);

      // Spark 1, all Spark classes comes from the spark-assembly jar
      // Spark 2+, all Spark classes should comes from the spark-lib directory
      if (classPathURL.equals(sparkConfURL) || sparkLibURL.equals(getParentURL(classPathURL))) {
        continue;
      }
      LOG.info("Removing duplicated url from classpath {}", classPathURL);
      urls.remove(classPathURL);
    }
  }

  private static Object createLogger(ClassLoader classLoader) throws Exception {
    return classLoader.loadClass(LoggerFactory.class.getName())
        .getMethod("getLogger", Class.class)
        .invoke(null, SparkContainerLauncher.class);
  }

  private static void log(Object logger, String level, String message, Object... args) {
    try {
      logger.getClass().getMethod(level, String.class, Object[].class)
          .invoke(logger, message, args);
    } catch (Exception e) {
      // ignore
    }
  }

  private static URL getParentURL(URL url) throws URISyntaxException, MalformedURLException {
    URI uri = url.toURI();
    return (uri.getPath().endsWith("/") ? uri.resolve("..") : uri.resolve(".")).toURL();
  }
}
