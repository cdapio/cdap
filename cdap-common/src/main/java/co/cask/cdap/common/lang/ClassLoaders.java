/*
 * Copyright © 2014 Cask Data, Inc.
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

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collection;
import javax.annotation.Nullable;

/**
 * Utility class for collection of methods for dealing with ClassLoader and loading class.
 */
public final class ClassLoaders {

  private ClassLoaders() { }

  /**
   * Loads the class with the given class name with the given classloader. If it is {@code null},
   * load the class with the ClassLoader of the caller object.
   *
   * @param className Name of the class to load.
   * @param classLoader Classloader for loading the class. It could be {@code null}.
   * @param caller The object who call this method.
   * @return The loaded class.
   * @throws ClassNotFoundException If failed to load the given class.
   */
  public static Class<?> loadClass(String className, @Nullable ClassLoader classLoader,
                                   Object caller) throws ClassNotFoundException {
    ClassLoader cl = Objects.firstNonNull(classLoader, caller.getClass().getClassLoader());
    return cl.loadClass(className);
  }

  /**
   * Sets the context ClassLoader and returns the current ClassLoader.
   */
  public static ClassLoader setContextClassLoader(ClassLoader classLoader) {
    ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(classLoader);
    return oldClassLoader;
  }

  /**
   * Finds a ClassLoader along the ClassLoader hierarchy that is assignable from the given type.
   * This method recognize usage of {@link Delegator} on ClassLoader and will try to match with
   * the object returned by {@link Delegator#getDelegate()} for the ClassLoader of the given type.
   *
   * @return the ClassLoader found or {@code null} if no such ClassLoader exists.
   */
  @SuppressWarnings("unchecked")
  @Nullable
  public static <T extends ClassLoader> T find(@Nullable ClassLoader classLoader, Class<T> type) {
    ClassLoader result = classLoader;
    while (result != null) {
      if (result instanceof Delegator) {
        Object delegate = ((Delegator) result).getDelegate();
        if (delegate != null && delegate instanceof ClassLoader) {
          result = (ClassLoader) delegate;
        }
      }

      if (type.isAssignableFrom(result.getClass())) {
        break;
      }
      result = result.getParent();
    }
    // The casting should succeed since it's either null or is assignable to the given type
    return (T) result;
  }

  /**
   * Populates the list of {@link URL} that this ClassLoader uses, including all URLs used by the parent of the
   * given ClassLoader.
   *
   * @param urls a {@link Collection} for storing the {@link URL}s
   * @return the same {@link Collection} passed from the parameter
   */
  public static <T extends Collection<? super URL>> T getClassLoaderURLs(ClassLoader classLoader, T urls) {
    if (classLoader.getParent() != null) {
      getClassLoaderURLs(classLoader.getParent(), urls);
    }

    if (classLoader instanceof URLClassLoader) {
      urls.addAll(Arrays.asList(((URLClassLoader) classLoader).getURLs()));
    }
    return urls;
  }

  /**
   * Creates a {@link Function} that perform class name to class resource lookup.
   *
   * @param classLoader the {@link ClassLoader} to use for the resource lookup.
   * @return the {@link URL} contains the class file or {@code null} if the resource is not found.
   */
  @Nullable
  public static Function<String, URL> createClassResourceLookup(final ClassLoader classLoader) {
    return new Function<String, URL>() {
      @Nullable
      @Override
      public URL apply(String className) {
        return classLoader.getResource(className.replace('.', '/') + ".class");
      }
    };
  }

  /**
   * Returns the URL to the base classpath for the given class resource URL of the given class name.
   */
  public static URL getClassPathURL(String className, URL classUrl) {
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
