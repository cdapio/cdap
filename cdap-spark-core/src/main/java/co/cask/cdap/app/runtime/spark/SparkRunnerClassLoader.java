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

import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.app.runtime.spark.classloader.SparkClassRewriter;
import co.cask.cdap.common.internal.guava.ClassPath;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.ClassPathResources;
import co.cask.cdap.internal.app.runtime.spark.SparkUtils;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A special {@link ClassLoader} for defining and loading all cdap-spark-core classes and Spark classes.
 *
 * IMPORTANT: Due to discovery in CDAP-5822, don't use getResourceAsStream in this class.
 */
public final class SparkRunnerClassLoader extends URLClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRunnerClassLoader.class);

  // Set of resources that are in cdap-api. They should be loaded by the parent ClassLoader
  private static final Set<String> API_CLASSES;

  private final SparkClassRewriter rewriter;

  static {
    Set<String> apiClasses = new HashSet<>();
    try {
      Iterables.addAll(
        apiClasses, Iterables.transform(
          Iterables.filter(ClassPathResources.getClassPathResources(Spark.class.getClassLoader(), Spark.class),
                           ClassPath.ClassInfo.class),
          ClassPathResources.CLASS_INFO_TO_CLASS_NAME
        )
      );
    } catch (IOException e) {
      // Shouldn't happen, because Spark.class is in cdap-api and it should always be there.
      LOG.error("Unable to find cdap-api classes.", e);
    }

    API_CLASSES = Collections.unmodifiableSet(apiClasses);
  }

  private static URL[] getClassloaderURLs(ClassLoader classLoader) throws IOException {
    List<URL> urls = ClassLoaders.getClassLoaderURLs(classLoader, new ArrayList<URL>());

    // If Spark classes are not available in the given ClassLoader, try to locate the Spark assembly jar
    // This class cannot have dependency on Spark directly, hence using the class resource to discover if SparkContext
    // is there
    if (classLoader.getResource("org/apache/spark/SparkContext.class") == null) {
      urls.add(SparkUtils.locateSparkAssemblyJar().toURI().toURL());
    }
    return urls.toArray(new URL[urls.size()]);
  }

  public SparkRunnerClassLoader(ClassLoader parent, boolean rewriteYarnClient) throws IOException {
    this(getClassloaderURLs(parent), parent, rewriteYarnClient);
  }

  public SparkRunnerClassLoader(URL[] urls, @Nullable ClassLoader parent, boolean rewriteYarnClient) {
    super(urls, parent);
    this.rewriter = new SparkClassRewriter(new Function<String, URL>() {
      @Nullable
      @Override
      public URL apply(String resourceName) {
        return findResource(resourceName);
      }
    }, rewriteYarnClient);
  }

  @Override
  protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    // We won't define the class with this ClassLoader for the following classes since they should
    // come from the parent ClassLoader
    // cdap-api-classes
    // Any class that is not from cdap-api-spark or cdap-spark-core
    // Any class that is not from Spark
    // We also need to define the org.spark-project., fastxml and akka classes in this ClassLoader
    // to avoid reference/thread leakage due to Spark assumption on process terminating after execution
    if (API_CLASSES.contains(name) || (!name.startsWith("co.cask.cdap.api.spark.")
        && !name.startsWith("co.cask.cdap.app.runtime.spark.")
        && !name.startsWith("org.apache.spark.") && !name.startsWith("org.spark-project.")
        && !name.startsWith("com.fasterxml.jackson.module.scala.")
        && !name.startsWith("akka.") && !name.startsWith("com.typesafe."))) {
      return super.loadClass(name, resolve);
    }

    // If the class is already loaded, return it
    Class<?> cls = findLoadedClass(name);
    if (cls != null) {
      return cls;
    }

    // Define the class with this ClassLoader
    try (InputStream is = openResource(name.replace('.', '/') + ".class")) {
      if (is == null) {
        throw new ClassNotFoundException("Failed to find resource for class " + name);
      }

      byte[] byteCode = rewriter.rewriteClass(name, is);

      // If no rewrite was performed, just define the class with this classloader by calling findClass.
      if (byteCode == null) {
        cls = findClass(name);
      } else {
        cls = defineClass(name, byteCode, 0, byteCode.length);
      }

      if (resolve) {
        resolveClass(cls);
      }
      return cls;
    } catch (IOException e) {
      throw new ClassNotFoundException("Failed to read class definition for class " + name, e);
    }
  }

  /**
   * Returns an {@link InputStream} to the given resource or {@code null} if cannot find the given resource
   * with this {@link ClassLoader}.
   */
  @Nullable
  private InputStream openResource(String resourceName) throws IOException {
    URL resource = findResource(resourceName);
    return resource == null ? null : resource.openStream();
  }
}
