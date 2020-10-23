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

package io.cdap.cdap.app.runtime.spark.classloader;

import io.cdap.cdap.api.spark.Spark;
import io.cdap.cdap.common.internal.guava.ClassPath;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.lang.ClassPathResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * A special {@link ClassLoader} for defining and loading all cdap-spark-core classes and Spark classes.
 * This {@link ClassLoader} is used for Spark execution in SDK as well as the client container in
 * distributed mode. For Spark containers (driver and executors), the {@link SparkContainerClassLoader} is used
 * instead.
 *
 * IMPORTANT: Due to discovery in CDAP-5822, don't use getResourceAsStream in this class.
 * IMPORTANT 2: Due to failure observed in CDAP-14062, use getResource() instead of findResource().
 * The difference is that getResource() will search from the parent ClassLoader first and return if found,
 * while findResource() will always look for it from the current ClassLoader.
 * In unit-test and SDK, spark classes are available from the runtime extension system, hence are available from the
 * parent of this ClassLoader. By using URL returned from the parent, which will never get closed, it eliminates the
 * potential issue of the input stream getting closed when a different instance of this class is being closed.
 * Underneath Java has a cache for JarFile opened from URL that represents an entry inside a jar file,
 * which apparently closing one InputStream could affect another InputStream opened for the same jar file entry.
 */
public final class SparkRunnerClassLoader extends URLClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRunnerClassLoader.class);

  // Set of resources that are in cdap-api. They should be loaded by the parent ClassLoader
  private static final Set<String> API_CLASSES;

  // A closeables for keeping track of streams opened via getResourceAsStream
  private final Map<Closeable, Void> closeables;
  private final Lock closeablesLock;
  private final SparkClassRewriter rewriter;

  static {
    Set<String> apiClasses = Collections.emptySet();
    try {
      apiClasses = ClassPathResources.getClassPathResources(Spark.class.getClassLoader(), Spark.class).stream()
        .filter(ClassPath.ClassInfo.class::isInstance)
        .map(ClassPath.ClassInfo.class::cast)
        .map(ClassPath.ClassInfo::getName)
        .collect(Collectors.toSet());
    } catch (IOException e) {
      // Shouldn't happen, because Spark.class is in cdap-api and it should always be there.
      LOG.error("Unable to find cdap-api classes.", e);
    }

    API_CLASSES = Collections.unmodifiableSet(apiClasses);
  }

  public SparkRunnerClassLoader(URL[] urls, @Nullable ClassLoader parent, boolean rewriteYarnClient,
                                boolean rewriteCheckpointTempFileName) {
    super(urls, parent);
    // Copy from URLClassLoader, which also uses WeakHashMap
    this.closeables = new WeakHashMap<>();
    this.closeablesLock = new ReentrantLock();
    this.rewriter = new SparkClassRewriter(name -> ClassLoaders.openResource(this, name),
                                           rewriteYarnClient,
                                           rewriteCheckpointTempFileName);
  }

  @Override
  public InputStream getResourceAsStream(String name) {
    URL url = getResource(name);
    if (url == null) {
      return null;
    }

    try {
      URLConnection urlConn = url.openConnection();
      urlConn.setUseCaches(false);
      InputStream is = urlConn.getInputStream();
      closeablesLock.lock();
      try {
        closeables.putIfAbsent(is, null);
      } finally {
        closeablesLock.unlock();
      }

      return is;

    } catch (IOException e) {
      return null;
    }
  }

  @Override
  public void close() throws IOException {
    IOException ex = null;
    try {
      super.close();
    } catch (IOException e) {
      ex = e;
    }

    // Close all the InputStreams acquired via getResourceAsStream
    closeablesLock.lock();
    try {
      for (Closeable c : closeables.keySet()) {
        try {
          c.close();
        } catch (IOException e) {
          if (ex == null) {
            ex = e;
          } else {
            ex.addSuppressed(e);
          }
        }
      }
      closeables.clear();
    } finally {
      closeablesLock.unlock();
    }

    if (ex != null) {
      throw ex;
    }
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
    if (API_CLASSES.contains(name) || (!name.startsWith("io.cdap.cdap.api.spark.")
        && !name.startsWith("io.cdap.cdap.app.deploy.spark.")
        && !name.startsWith("io.cdap.cdap.app.runtime.spark.")
        && !name.startsWith("org.apache.spark.") && !name.startsWith("org.spark-project.")
        && !name.startsWith("akka.") && !name.startsWith("com.typesafe.")
        && !name.startsWith("com.esotericsoftware.")    // For Kryo and reflectasm
        && !name.startsWith("com.twitter.chill."))
        && !name.startsWith("org.codehaus.janino.")
        && !name.startsWith("com.fasterxml.jackson.")
        && !name.startsWith("com.codahale.metrics.")
        && !name.startsWith("py4j.")
      ) {
      return super.loadClass(name, resolve);
    }

    synchronized (getClassLoadingLock(name)) {
      // If the class is already loaded, return it
      Class<?> cls = findLoadedClass(name);
      if (cls != null) {
        return cls;
      }

      // Define the class with this ClassLoader
      try (InputStream is = ClassLoaders.openResource(this, name.replace('.', '/') + ".class")) {
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
  }
}
