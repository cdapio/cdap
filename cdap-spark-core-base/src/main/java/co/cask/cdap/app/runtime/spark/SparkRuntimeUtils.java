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

import co.cask.cdap.app.runtime.spark.classloader.SparkRunnerClassLoader;
import co.cask.cdap.common.internal.guava.ClassPath;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.ClassPathResources;
import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.common.lang.WeakReferenceDelegatorClassLoader;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.io.OutputSupplier;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.DStreamGraph;
import org.apache.spark.streaming.StreamingContext;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.parallel.TaskSupport;
import scala.collection.parallel.ThreadPoolTaskSupport;
import scala.collection.parallel.mutable.ParArray;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Util class for common functions needed for Spark implementation.
 */
public final class SparkRuntimeUtils {

  private static final String LOCALIZED_RESOURCES = "spark.cdap.localized.resources";

  private static final Logger LOG = LoggerFactory.getLogger(SparkRuntimeUtils.class);
  private static final Gson GSON = new Gson();

  // ClassLoader filter
  @VisibleForTesting
  public static final FilterClassLoader.Filter SPARK_PROGRAM_CLASS_LOADER_FILTER = new FilterClassLoader.Filter() {

    final FilterClassLoader.Filter defaultFilter = FilterClassLoader.defaultFilter();
    volatile Set<ClassPath.ResourceInfo> sparkStreamingResources;

    @Override
    public boolean acceptResource(final String resource) {
      // All Spark API, Spark, Scala, Akka and Kryo classes should come from parent.
      if (resource.startsWith("co/cask/cdap/api/spark/")) {
        return true;
      }
      if (resource.startsWith("scala/")) {
        return true;
      }
      if (resource.startsWith("akka/")) {
        return true;
      }
      if (resource.startsWith("com/esotericsoftware/kryo/")) {
        return true;
      }
      if (resource.startsWith("org/apache/spark/")) {
        // Only allows the core Spark Streaming classes, but not any streaming extensions (like Kafka).
        // cdh 5.5+ package streaming kafka and flume into their spark assembly jar, but they don't package their
        // dependencies. For example, streaming kafka is packaged, but kafka is not.
        if (resource.startsWith("org/apache/spark/streaming/kafka") ||
          resource.startsWith("org/apache/spark/streaming/flume")) {
          return false;
        }
        if (resource.startsWith("org/apache/spark/streaming")) {
          return Iterables.any(getSparkStreamingResources(), new Predicate<ClassPath.ResourceInfo>() {
            @Override
            public boolean apply(ClassPath.ResourceInfo input) {
              return input.getResourceName().equals(resource);
            }
          });
        }
        return true;
      }
      if (resource.startsWith("com/google/common/base/Optional")) {
        return true;
      }
      return defaultFilter.acceptResource(resource);
    }

    @Override
    public boolean acceptPackage(final String packageName) {
      if (packageName.equals("co.cask.cdap.api.spark") || packageName.startsWith("co.cask.cdap.api.spark.")) {
        return true;
      }
      if (packageName.equals("scala") || packageName.startsWith("scala.")) {
        return true;
      }
      if (packageName.equals("akka") || packageName.startsWith("akka.")) {
        return true;
      }
      if (packageName.equals("com.esotericsoftware.kryo") || packageName.startsWith("com.esotericsoftware.kryo.")) {
        return true;
      }
      // cdh 5.5 and on package kafka and flume streaming in their assembly jar
      if (packageName.equals("org.apache.spark") || packageName.startsWith("org.apache.spark.")) {
        // Only allows the core Spark Streaming classes, but not any streaming extensions (like Kafka).
        if (packageName.startsWith("org.apache.spark.streaming.kafka") ||
          packageName.startsWith("org.apache.spark.streaming.flume")) {
          return false;
        }
        if (packageName.equals("org.apache.spark.streaming") || packageName.startsWith("org.apache.spark.streaming.")) {
          return Iterables.any(
            Iterables.filter(getSparkStreamingResources(), ClassPath.ClassInfo.class),
            new Predicate<ClassPath.ClassInfo>() {
              @Override
              public boolean apply(ClassPath.ClassInfo input) {
                return input.getPackageName().equals(packageName);
              }
            });
        }
        return true;
      }
      return defaultFilter.acceptResource(packageName);
    }

    /**
     * Gets the set of resources information that are from the Spark Streaming Core. It excludes any
     * Spark streaming extensions, such as Kafka or Flume. They need to be excluded since they are not
     * part of Spark distribution and it should be loaded from the user program ClassLoader. This filtering
     * is needed for unit-testing because in unit-test, those extension classes are loadable from the system
     * classloader, causing same classes being loaded through different classloader.
     */
    private Set<ClassPath.ResourceInfo> getSparkStreamingResources() {
      if (sparkStreamingResources != null) {
        return sparkStreamingResources;
      }
      synchronized (this) {
        if (sparkStreamingResources != null) {
          return sparkStreamingResources;
        }

        try {
          sparkStreamingResources = ClassPathResources.getClassPathResources(getClass().getClassLoader(),
                                                                             StreamingContext.class);
        } catch (IOException e) {
          LOG.warn("Failed to find resources for Spark StreamingContext.", e);
          sparkStreamingResources = Collections.emptySet();
        }
        return sparkStreamingResources;
      }
    }
  };

  /**
   * Creates a zip file which contains a serialized {@link Properties} with a given zip entry name, together with
   * all files under the given directory. This is called from Client.createConfArchive() as a workaround for the
   * SPARK-13441 bug.
   *
   * @param sparkConf the {@link SparkConf} to save
   * @param propertiesEntryName name of the zip entry for the properties
   * @param confDirPath directory to scan for files to include in the zip file
   * @param outputZipPath output file
   * @return the zip file
   */
  public static File createConfArchive(SparkConf sparkConf, final String propertiesEntryName,
                                       String confDirPath, String outputZipPath) {
    final Properties properties = new Properties();
    for (Tuple2<String, String> tuple : sparkConf.getAll()) {
      properties.put(tuple._1(), tuple._2());
    }

    try {
      File confDir = new File(confDirPath);
      final File zipFile = new File(outputZipPath);
      BundleJarUtil.createArchive(confDir, new OutputSupplier<ZipOutputStream>() {
        @Override
        public ZipOutputStream getOutput() throws IOException {
          ZipOutputStream zipOutput = new ZipOutputStream(new FileOutputStream(zipFile));
          zipOutput.putNextEntry(new ZipEntry(propertiesEntryName));
          properties.store(zipOutput, "Spark configuration.");
          zipOutput.closeEntry();

          return zipOutput;
        }
      });
      LOG.debug("Spark config archive created at {} from {}", zipFile, confDir);
      return zipFile;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Sets the context ClassLoader to the given {@link SparkClassLoader}. It will also set the
   * ClassLoader for the {@link Configuration} contained inside the {@link SparkClassLoader}.
   *
   * @return a {@link Cancellable} to reset the classloader to the one prior to the call
   */
  public static Cancellable setContextClassLoader(final SparkClassLoader sparkClassLoader) {
    final Configuration hConf = sparkClassLoader.getRuntimeContext().getConfiguration();
    final ClassLoader oldConfClassLoader = hConf.getClassLoader();

    // Always wrap it with WeakReference to avoid ClassLoader leakage from Spark.
    ClassLoader classLoader = new WeakReferenceDelegatorClassLoader(sparkClassLoader);
    hConf.setClassLoader(classLoader);
    final ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(classLoader);
    return new Cancellable() {
      @Override
      public void cancel() {
        hConf.setClassLoader(oldConfClassLoader);
        ClassLoaders.setContextClassLoader(oldClassLoader);

        // Do not remove the next line.
        // This is necessary to keep a strong reference to the SparkClassLoader so that it won't get GC until this
        // cancel() is called
        LOG.trace("Reset context ClassLoader. The SparkClassLoader is: {}", sparkClassLoader);
      }
    };
  }

  /**
   * Sets the {@link TaskSupport} for the given Scala {@link ParArray} to {@link ThreadPoolTaskSupport}.
   * This method is mainly used by {@link SparkRunnerClassLoader} to set the {@link TaskSupport} for the
   * parallel array used inside the {@link DStreamGraph} class in spark to avoid thread leakage after the
   * Spark program execution finished.
   */
  @SuppressWarnings("unused")
  public static <T> ParArray<T> setTaskSupport(ParArray<T> parArray) {
    ThreadPoolExecutor executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 1,
                                                         TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
                                                         new ThreadFactoryBuilder()
                                                           .setNameFormat("task-support-%d").build());
    executor.allowCoreThreadTimeOut(true);
    parArray.tasksupport_$eq(new ThreadPoolTaskSupport(executor));
    return parArray;
  }

  /**
   * Saves the names of localized resources to the given config.
   */
  public static void setLocalizedResources(Set<String> localizedResourcesNames,
                                           Map<String, String> configs) {
    configs.put(LOCALIZED_RESOURCES, GSON.toJson(localizedResourcesNames));
  }

  /**
   * Retrieves the names of localized resources in the given config and constructs a map from the resource name
   * to local files with the resource names as the file names in the given directory.
   */
  public static Map<String, File> getLocalizedResources(File dir, SparkConf sparkConf) {
    String resources = sparkConf.get(LOCALIZED_RESOURCES, null);
    if (resources == null) {
      return Collections.emptyMap();
    }
    Map<String, File> result = new HashMap<>();
    Set<String> resourceNames = GSON.fromJson(resources, new TypeToken<Set<String>>() { }.getType());
    for (String name : resourceNames) {
      result.put(name, new File(dir, name));
    }
    return result;
  }

  private SparkRuntimeUtils() {
    // private
  }
}
