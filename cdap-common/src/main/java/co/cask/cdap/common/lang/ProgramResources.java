/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.common.lang;

import co.cask.cdap.api.app.Application;
import co.cask.cdap.common.internal.guava.ClassPath;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.Path;

/**
 * Helper class to maintain list of resources that are visible to user programs.
 */
public final class ProgramResources {

  private static final Logger LOG = LoggerFactory.getLogger(ProgramResources.class);

  private static final List<String> HADOOP_PACKAGES = ImmutableList.of("org.apache.hadoop.");
  private static final List<String> HBASE_PACKAGES = ImmutableList.of("org.apache.hadoop.hbase.");
  private static final List<String> SPARK_PACKAGES = ImmutableList.of("org.apache.spark.", "scala.");

  private static final Predicate<URI> JAR_ONLY_URI = new Predicate<URI>() {
    @Override
    public boolean apply(URI input) {
      return input.getPath().endsWith(".jar");
    }
  };

  // Each program type has it's own set of visible resources
  private static Map<ProgramType, Set<String>> visibleResources = Maps.newHashMap();
  // Contains set of resources that are always visible to all program type.
  private static Set<String> baseResources;

  /**
   * Returns a Set of resource names that are visible through to user program.
   *
   * @param classLoader the ClassLoader for finding program type specific resources.
   * @param type program type. If {@code null}, only the base visible resources will be returned.
   */
  public static synchronized Set<String> getVisibleResources(ClassLoader classLoader, @Nullable ProgramType type) {
    if (type == null) {
      return getBaseResources();
    }

    Set<String> resources = visibleResources.get(type);
    if (resources != null) {
      return resources;
    }
    try {
      resources = createVisibleResources(classLoader, type);
    } catch (IOException e) {
      LOG.error("Failed to determine visible resources to user program of type {}", type, e);
      resources = ImmutableSet.of();
    }
    visibleResources.put(type, resources);
    return resources;
  }

  private static Set<String> createVisibleResources(ClassLoader classLoader, ProgramType type) throws IOException {
    Set<String> resources = getBaseResources();

    // Base on the type, add extra resources
    // Current only Spark and Workflow type has extra visible resources
    if (type == ProgramType.SPARK || type == ProgramType.WORKFLOW) {
      resources = getResources(ClassPath.from(classLoader, JAR_ONLY_URI),
                               SPARK_PACKAGES, ImmutableList.<String>of(),
                               ClassPathResources.RESOURCE_INFO_TO_RESOURCE_NAME, Sets.newHashSet(resources));
    }
    return Collections.unmodifiableSet(resources);
  }

  private static Set<String> getBaseResources() {
    if (baseResources != null) {
      return baseResources;
    }
    try {
      baseResources = createBaseResources();
    } catch (IOException e) {
      LOG.error("Failed to determine base visible resources to user program", e);
      baseResources = ImmutableSet.of();
    }
    return baseResources;
  }

  /**
   * Returns a Set of resources name that are visible through the cdap-api module as well as Hadoop classes.
   * This includes all classes+resources in cdap-api plus all classes+resources that cdap-api
   * depends on (for example, sl4j, guava, gson, etc).
   */
  private static Set<String> createBaseResources() throws IOException {
    // Everything should be traceable in the same ClassLoader of this class, which is the CDAP system ClassLoader
    ClassLoader classLoader = ProgramResources.class.getClassLoader();

    // Gather resources information for cdap-api classes
    // Add everything in cdap-api as visible resources
    // Trace dependencies for cdap-api classes
    Set<String> result = ClassPathResources.getResourcesWithDependencies(classLoader, Application.class);

    // Gather resources for javax.ws.rs classes. They are not traceable from the api classes.
    Iterables.addAll(result, Iterables.transform(ClassPathResources.getClassPathResources(classLoader, Path.class),
                                                 ClassPathResources.RESOURCE_INFO_TO_RESOURCE_NAME));

    // Gather Hadoop classes and resources
    getResources(ClassPath.from(classLoader, JAR_ONLY_URI),
                 HADOOP_PACKAGES, HBASE_PACKAGES, ClassPathResources.RESOURCE_INFO_TO_RESOURCE_NAME, result);

    return Collections.unmodifiableSet(result);
  }

  /**
   * Finds all resources that are accessible in a given {@link ClassPath} that starts with certain package prefixes.
   * Also includes all non .class file resources in the same base URLs of those classes that are accepted through
   * the package prefixes filtering.
   *
   * Resources information presented in the result collection is transformed by the given result transformation
   * function.
   */
  private static <V, T extends Collection<V>> T getResources(ClassPath classPath,
                                                             Iterable<String> includePackagePrefixes,
                                                             Iterable<String> excludePackagePrefixes,
                                                             Function<ClassPath.ResourceInfo, V> resultTransform,
                                                             final T result) throws IOException {
    Set<URL> resourcesBaseURLs = new HashSet<>();
    // Adds all .class resources that should be included
    // Also record the base URL of those resources
    for (ClassPath.ClassInfo classInfo : classPath.getAllClasses()) {
      boolean include = false;
      for (String prefix : includePackagePrefixes) {
        if (classInfo.getName().startsWith(prefix)) {
          include = true;
          break;
        }
      }
      for (String prefix : excludePackagePrefixes) {
        if (classInfo.getName().startsWith(prefix)) {
          include = false;
          break;
        }
      }

      if (include) {
        result.add(resultTransform.apply(classInfo));
        resourcesBaseURLs.add(classInfo.baseURL());
      }
    }

    // Adds non .class resources that are in the resourceBaseURLs
    for (ClassPath.ResourceInfo resourceInfo : classPath.getResources()) {
      if (resourceInfo instanceof ClassPath.ClassInfo) {
        // We already processed all classes in the loop above
        continue;
      }
      // See if the resource base URL is already accepted through class filtering.
      // If it does, adds the resource name to the collection as well.
      if (resourcesBaseURLs.contains(resourceInfo.baseURL())) {
        result.add(resultTransform.apply(resourceInfo));
      }
    }

    return result;
  }

  private ProgramResources() {
  }
}
