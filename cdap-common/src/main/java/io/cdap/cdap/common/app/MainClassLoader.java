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

package io.cdap.cdap.common.app;

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.common.dataset.DatasetClassRewriter;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.lang.CombineClassLoader;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.common.lang.GuavaClassRewriter;
import io.cdap.cdap.common.lang.InterceptableClassLoader;
import io.cdap.cdap.common.security.AuthEnforceRewriter;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.asm.Classes;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * The main {@link ClassLoader} used by CDAP. This class performs necessary class rewriting for the whole CDAP
 * system.
 */
public class MainClassLoader extends InterceptableClassLoader {

  private static final String DATASET_CLASS_NAME = Dataset.class.getName();
  private final GuavaClassRewriter guavaClassRewriter;
  private final DatasetClassRewriter datasetRewriter;
  private final AuthEnforceRewriter authEnforceRewriter;
  private final Function<String, URL> resourceLookup;
  private final Map<String, Boolean> cache;

  /**
   * @param extraClasspath extra list of {@link URL} to be added to the end of the classpath for the
   *                       {@link MainClassLoader} to be created
   * @return a new instance from the current context classloader or the system classloader. The returned
   * {@link MainClassLoader} will be the defining classloader for all classes available in the context classloader.
   * It will return {@code null} if it is not able to create a new instance due to lack of classpath information.
   *
   */
  @Nullable
  public static MainClassLoader createFromContext(URL...extraClasspath) {
    return createFromContext(new FilterClassLoader.Filter() {
      @Override
      public boolean acceptResource(String resource) {
        return false;
      }

      @Override
      public boolean acceptPackage(String packageName) {
        return false;
      }
    }, extraClasspath);
  }

  /**
   * @param filter A {@link FilterClassLoader.Filter} for filtering out classes from the
   * @param extraClasspath extra list of {@link URL} to be added to the end of the classpath for the
   *                       {@link MainClassLoader} to be created
   * @return a new instance from the current context classloader or the system classloader. The returned
   * {@link MainClassLoader} will be the defining classloader for classes in the context classloader
   * that the filter rejected. For classes that pass the filter, the defining classloader will be the original
   * context classloader.
   * It will return {@code null} if it is not able to create a new instance due to lack of classpath information.
   */
  @Nullable
  public static MainClassLoader createFromContext(FilterClassLoader.Filter filter, URL...extraClasspath) {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = ClassLoader.getSystemClassLoader();
    }

    List<URL> classpath = new ArrayList<>();

    if (classLoader instanceof URLClassLoader) {
      classpath.addAll(Arrays.asList(((URLClassLoader) classLoader).getURLs()));
    } else if (classLoader == ClassLoader.getSystemClassLoader()) {
      addClassPath(classpath);
    } else {
      // No able to create a new MainClassLoader
      return null;
    }

    classpath.addAll(Arrays.asList(extraClasspath));

    // Find and move hive-exec to the end. The hive-exec contains a lot of conflicting classes that we don't
    // want to include during dependency tracing.
    Iterator<URL> iterator = classpath.iterator();
    List<URL> hiveExecJars = new ArrayList<>();
    while (iterator.hasNext()) {
      URL url = iterator.next();
      if (url.getPath().contains("hive-exec")) {
        iterator.remove();
        hiveExecJars.add(url);
      }
    }
    classpath.addAll(hiveExecJars);

    ClassLoader filtered = new FilterClassLoader(classLoader, filter);
    ClassLoader parent = new CombineClassLoader(classLoader.getParent(), filtered);
    return new MainClassLoader(classpath.toArray(new URL[0]), parent);
  }

  /**
   * Creates a new instance for the following set of {@link URL}.
   *
   * @param urls the URLs from which to load classes and resources
   * @param parent the parent classloader for delegation
   */
  public MainClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
    this.guavaClassRewriter = new GuavaClassRewriter();
    this.datasetRewriter = new DatasetClassRewriter();
    this.authEnforceRewriter = new AuthEnforceRewriter();
    this.resourceLookup = ClassLoaders.createClassResourceLookup(this);
    this.cache = new HashMap<>();
  }

  @Override
  protected boolean needIntercept(String className) {
    try {
      return isRewriteNeeded(className);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Nullable
  @Override
  public byte[] rewriteClass(String className, InputStream input) throws IOException {
    byte[] rewrittenCode = null;

    if (guavaClassRewriter.needRewrite(className)) {
      rewrittenCode = guavaClassRewriter.rewriteClass(className, input);
    }

    if (isDatasetRewriteNeeded(className)) {
      rewrittenCode = datasetRewriter.rewriteClass(className, input);
    }

    if (isAuthRewriteNeeded(className)) {
      rewrittenCode = authEnforceRewriter.rewriteClass(
        className, rewrittenCode == null ? input : new ByteArrayInputStream(rewrittenCode));
    }
    return rewrittenCode;
  }

  /**
   * Adds {@link URL} to the given list based on the system classpath.
   */
  private static void addClassPath(List<URL> urls) {
    String wildcardSuffix = File.pathSeparator + "*";
    // In case the system classloader is not a URLClassLoader, use the classpath property (maybe from non Oracle JDK)
    for (String path : Splitter.on(File.pathSeparatorChar).split(System.getProperty("java.class.path"))) {
      if ("*".equals(path) || path.endsWith(wildcardSuffix)) {
        for (File jarFile : DirUtils.listFiles(new File(path), "jar")) {
          try {
            urls.add(jarFile.toURI().toURL());
          } catch (MalformedURLException e) {
            // Shouldn't happen. Propagate the exception.
            throw Throwables.propagate(e);
          }
        }
      }
    }
  }

  private boolean isRewriteNeeded(String className) throws IOException {
    return guavaClassRewriter.needRewrite(className)
      || isDatasetRewriteNeeded(className)
      || isAuthRewriteNeeded(className);
  }

  private boolean isDatasetRewriteNeeded(String className) throws IOException {
    return Classes.isSubTypeOf(className, DATASET_CLASS_NAME, resourceLookup, cache);
  }

  private boolean isAuthRewriteNeeded(String className) {
    return className.startsWith("io.cdap.cdap.");
  }
}
