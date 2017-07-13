/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark.classloader;

import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.common.internal.guava.ClassPath;
import co.cask.cdap.common.lang.ClassPathResources;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A special {@link ClassLoader} for defining and loading all cdap-spark-core classes and Spark classes.
 * This {@link ClassLoader} is used for Spark execution in SDK as well as the client container in
 * distributed mode. For Spark containers (driver and executors), the {@link SparkContainerClassLoader} is used
 * instead.
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
    // Also need to define Kryo class as it is for rewriting the Kryo class to add extra default serializers
    // Also need to define janino class for code compilation to avoid leaking classloader
    // Also the fasterxml and codahale classes to avoid leaking classloader when Spark SQL is used;
    // it has a LRUCache inside that would caches object class.
    if (API_CLASSES.contains(name) || (!name.startsWith("co.cask.cdap.api.spark.")
        && !name.startsWith("co.cask.cdap.app.deploy.spark.")
        && !name.startsWith("co.cask.cdap.app.runtime.spark.")
        && !name.startsWith("co.cask.cdap.spark.sql.")
        && !name.startsWith("org.apache.spark.") && !name.startsWith("org.spark-project.")
        && !name.startsWith("akka.") && !name.startsWith("com.typesafe.")
        && !name.startsWith("com.esotericsoftware.")    // For Kryo and reflectasm
        && !name.startsWith("com.twitter.chill."))
        && !name.startsWith("org.codehaus.janino.")
        && !name.startsWith("com.fasterxml.jackson.")
        && !name.startsWith("com.codahale.metrics.")
      ) {
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
