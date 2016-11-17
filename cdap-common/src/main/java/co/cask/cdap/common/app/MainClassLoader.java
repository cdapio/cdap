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

package co.cask.cdap.common.app;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.common.dataset.DatasetClassRewriter;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.common.lang.InterceptableClassLoader;
import co.cask.cdap.common.security.AuthEnforceRewriter;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.asm.Classes;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * The main {@link ClassLoader} used by CDAP. This class performs necessary class rewriting for the whole CDAP
 * system.
 */
public final class MainClassLoader extends InterceptableClassLoader {

  private static final String DATASET_CLASS_NAME = Dataset.class.getName();
  private final DatasetClassRewriter datasetRewriter;
  private final AuthEnforceRewriter authEnforceRewriter;
  private final Function<String, URL> resourceLookup;
  private final Map<String, Boolean> cache;

  /**
   * @return a new instance from the current context classloader or the system classloader. The returned
   * {@link MainClassLoader} will be the defining classloader for all classes available in the context classloader.
   * It will return {@code null} if it is not able to create a new instance due to lack of classpath information.
   */
  @Nullable
  public static MainClassLoader createFromContext() {
    return createFromContext(new FilterClassLoader.Filter() {
      @Override
      public boolean acceptResource(String resource) {
        return false;
      }

      @Override
      public boolean acceptPackage(String packageName) {
        return false;
      }
    });
  }

  /**
   * @return a new instance from the current context classloader or the system classloader. The returned
   * {@link MainClassLoader} will be the defining classloader for classes in the context classloader
   * that the filter rejected. For classes that pass the filter, the defining classloader will be the original
   * context classloader.
   * It will return {@code null} if it is not able to create a new instance due to lack of classpath information.
   */
  @Nullable
  public static MainClassLoader createFromContext(FilterClassLoader.Filter filter) {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = ClassLoader.getSystemClassLoader();
    }

    URL[] classpath;

    if (classLoader instanceof URLClassLoader) {
      classpath = ((URLClassLoader) classLoader).getURLs();
    } else if (classLoader == ClassLoader.getSystemClassLoader()) {
      classpath = getClassPath();
    } else {
      // No able to create a new MainClassLoader
      return null;
    }

    ClassLoader filtered = new FilterClassLoader(classLoader, filter);
    ClassLoader parent = new CombineClassLoader(classLoader.getParent(), Collections.singleton(filtered));
    return new MainClassLoader(classpath, parent);
  }

  /**
   * Creates a new instance for the following set of {@link URL}.
   *
   * @param urls the URLs from which to load classes and resources
   * @param parent the parent classloader for delegation
   */
  public MainClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
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

  @Override
  public byte[] rewriteClass(String className, InputStream input) throws IOException {
    if (isDatasetRewriteNeeded(className)) {
      input = new ByteArrayInputStream(datasetRewriter.rewriteClass(className, input));
    }
    if (isAuthRewriteNeeded(className)) {
      input = new ByteArrayInputStream(authEnforceRewriter.rewriteClass(className, input));
    }
    return ByteStreams.toByteArray(input);
  }

  /**
   * Returns an array of {@link URL} based on the system classpath.
   */
  private static URL[] getClassPath() {
    List<URL> urls = new ArrayList<>();

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

    return urls.toArray(new URL[urls.size()]);
  }

  private boolean isRewriteNeeded(String className) throws IOException {
    return isDatasetRewriteNeeded(className) || isAuthRewriteNeeded(className);
  }

  private boolean isDatasetRewriteNeeded(String className) throws IOException {
    return Classes.isSubTypeOf(className, DATASET_CLASS_NAME, resourceLookup, cache);
  }

  private boolean isAuthRewriteNeeded(String className) {
    return className.startsWith("co.cask.cdap.");
  }
}
