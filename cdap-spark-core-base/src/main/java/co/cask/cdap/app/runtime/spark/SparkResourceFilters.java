/*
 * Copyright © 2018 Cask Data, Inc.
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

import co.cask.cdap.common.internal.guava.ClassPath;
import co.cask.cdap.common.lang.ClassPathResources;
import co.cask.cdap.common.lang.FilterClassLoader;
import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.streaming.StreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 * A util class for hosting filtering spark resources for classloader. It is in its own class to minizie the dependency
 * of this class.
 */
public final class SparkResourceFilters {

  private static final Logger LOG = LoggerFactory.getLogger(SparkResourceFilters.class);

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
          return getSparkStreamingResources().stream().anyMatch(info -> info.getResourceName().equals(resource));
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
          return getSparkStreamingResources().stream()
            .filter(ClassPath.ClassInfo.class::isInstance)
            .map(ClassPath.ClassInfo.class::cast)
            .anyMatch(info -> info.getPackageName().equals(packageName));
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
}
