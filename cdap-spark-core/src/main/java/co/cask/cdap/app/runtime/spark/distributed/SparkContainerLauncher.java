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

import co.cask.cdap.app.runtime.spark.SparkRunnerClassLoader;
import co.cask.cdap.app.runtime.spark.SparkRuntimeContextProvider;
import co.cask.cdap.common.app.MainClassLoader;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.logging.StandardOutErrorRedirector;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class launches Spark YARN containers with classes loaded through the {@link SparkRunnerClassLoader}.
 */
public final class SparkContainerLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(SparkContainerLauncher.class);

  /**
   * Launches the given main class. The main class will be loaded through the {@link SparkRunnerClassLoader}.
   *
   * @param mainClassName the main class to launch
   * @param args arguments for the main class
   */
  @SuppressWarnings("unused")
  public static void launch(String mainClassName, String[] args) {
    ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
    List<URL> urls = ClassLoaders.getClassLoaderURLs(systemClassLoader, new ArrayList<URL>());

    // Remove the URL that contains the given main classname to avoid infinite recursion.
    // This is needed because we generate a class with the same main classname in order to intercept the main()
    // method call from the container launch script.
    URL resource = systemClassLoader.getResource(mainClassName.replace('.', '/') + ".class");
    if (resource == null) {
      throw new IllegalStateException("Failed to find resource for main class " + mainClassName);
    }

    if (!urls.remove(getClassPathURL(mainClassName, resource))) {
      throw new IllegalStateException("Failed to remove main class resource " + resource);
    }

    // Creates the SparkRunnerClassLoader for class rewriting and it will be used for the rest of the execution.
    // Use the extension classloader as the parent instead of the system classloader because
    // Spark classes are in the system classloader which we want to rewrite.
    URL[] classLoaderUrls = urls.toArray(new URL[urls.size()]);
    ClassLoader classLoader = new SparkRunnerClassLoader(
      classLoaderUrls, new MainClassLoader(classLoaderUrls, systemClassLoader.getParent()), false, false);

    // Sets the context classloader and launch the actual Spark main class.
    Thread.currentThread().setContextClassLoader(classLoader);
    try {
      // Get the SparkRuntimeContext to initialize all necessary services and logging context
      // Need to do it using the SparkRunnerClassLoader through reflection.
      classLoader.loadClass(SparkRuntimeContextProvider.class.getName()).getMethod("get").invoke(null);
      // Invoke StandardOutErrorRedirector.redirectToLogger()
      classLoader.loadClass(StandardOutErrorRedirector.class.getName())
        .getDeclaredMethod("redirectToLogger", String.class)
        .invoke(null, mainClassName);

      LOG.info("Launching " + mainClassName + ".main(" + Arrays.toString(args) + ")");
      classLoader.loadClass(mainClassName).getMethod("main", String[].class).invoke(null, new Object[]{args});
    } catch (Exception e) {
      throw new RuntimeException("Failed to call " + mainClassName + ".main(String[])", e);
    }
  }

  private static URL getClassPathURL(String className, URL classUrl) {
    try {
      if ("file".equals(classUrl.getProtocol())) {
        String path = classUrl.getFile();
        // Compute the directory container the class.
        int endIdx = path.length() - className.length() - ".class".length();
        if (endIdx > 1) {
          // If it is not the root directory, return the end index to remove the trailing '/'.
          endIdx--;
        }
        return new URL("file", "", -1, path.substring(0, endIdx));
      }
      if ("jar".equals(classUrl.getProtocol())) {
        String path = classUrl.getFile();
        return URI.create(path.substring(0, path.indexOf("!/"))).toURL();
      }
    } catch (MalformedURLException e) {
      throw Throwables.propagate(e);
    }
    throw new IllegalStateException("Unsupported class URL: " + classUrl);
  }
}
