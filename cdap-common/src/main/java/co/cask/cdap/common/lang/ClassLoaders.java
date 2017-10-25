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
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Queue;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 * Utility class for collection of methods for dealing with ClassLoader and loading class.
 */
public final class ClassLoaders {

  // Class-Path attributed in JAR are " " separated
  private static final Splitter CLASS_PATH_ATTR_SPLITTER = Splitter.on(" ").omitEmptyStrings();

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
   * This method recognize usage of {@link Delegator} and {@link CombineClassLoader} and will try to match with
   * the object returned by {@link Delegator#getDelegate()} or {@link CombineClassLoader#getDelegates()}
   * for the ClassLoader of the given type.
   *
   * @return the ClassLoader found or {@code null} if no such ClassLoader exists.
   */
  @SuppressWarnings("unchecked")
  @Nullable
  public static <T extends ClassLoader> T find(@Nullable ClassLoader classLoader, Class<T> type) {
    if (classLoader == null) {
      return null;
    }

    Queue<ClassLoader> queue = new LinkedList<>();
    queue.add(classLoader);

    while (!queue.isEmpty()) {
      ClassLoader cl = queue.remove();
      if (type.isAssignableFrom(cl.getClass())) {
        return (T) cl;
      }
      if (cl instanceof Delegator) {
        Object delegate = ((Delegator) cl).getDelegate();
        if (delegate != null && delegate instanceof ClassLoader) {
          queue.add((ClassLoader) delegate);
        }
      }
      if (cl instanceof CombineClassLoader) {
        queue.addAll(((CombineClassLoader) cl).getDelegates());
      }
      if (cl.getParent() != null) {
        queue.add(cl.getParent());
      }
    }
    return null;
  }

  /**
   * Finds a ClassLoader along the ClassLoader hierarchy that the ClassLoader class matches the given name.
   * This method recognize usage of {@link Delegator} on ClassLoader and will try to match with
   * the object returned by {@link Delegator#getDelegate()} for the ClassLoader of the given type.
   *
   * @return the ClassLoader found or {@code null} if no such ClassLoader exists.
   */
  @Nullable
  public static ClassLoader findByName(@Nullable ClassLoader classLoader, String classLoaderName) {
    ClassLoader result = classLoader;
    while (result != null) {
      if (result instanceof Delegator) {
        Object delegate = ((Delegator) result).getDelegate();
        if (delegate != null && delegate instanceof ClassLoader) {
          result = (ClassLoader) delegate;
        }
      }

      if (result.getClass().getName().equals(classLoaderName)) {
        break;
      }
      result = result.getParent();
    }
    return result;
  }

  /**
   * Populates the list of {@link URL} that this ClassLoader uses, including all URLs used by the parent of the
   * given ClassLoader. This is the same as calling {@link #getClassLoaderURLs(ClassLoader, boolean, Collection)} with
   * {@code childFirst} set to {@code false}.
   *
   * @param classLoader the {@link ClassLoader} for searching urls
   * @param urls a {@link Collection} for storing the {@link URL}s
   * @return the same {@link Collection} passed from the parameter
   */
  public static <T extends Collection<? super URL>> T getClassLoaderURLs(ClassLoader classLoader, T urls) {
    return getClassLoaderURLs(classLoader, false, urls);
  }

  /**
   * Populates the list of {@link URL} that this ClassLoader uses, including all URLs used by the parent of the
   * given ClassLoader.
   *
   * @param classLoader the {@link ClassLoader} for searching urls
   * @param childFirst if {@code true}, urls gathered from the lower classloader in the hierarchy will be
   *                   added before the higher one.
   * @param urls a {@link Collection} for storing the {@link URL}s
   * @return the same {@link Collection} passed from the parameter
   */
  public static <T extends Collection<? super URL>> T getClassLoaderURLs(ClassLoader classLoader,
                                                                         boolean childFirst, T urls) {
    Deque<URLClassLoader> classLoaders = collectURLClassLoaders(classLoader, new LinkedList<URLClassLoader>());

    Iterator<URLClassLoader> iterator = childFirst ? classLoaders.iterator() : classLoaders.descendingIterator();
    while (iterator.hasNext()) {
      ClassLoader cl = iterator.next();
      for (URL url : ((URLClassLoader) cl).getURLs()) {
        if (urls.add(url) && (url.getProtocol().equals("file"))) {
          addClassPathFromJar(url, urls);
        }
      }
    }

    return urls;
  }

  /**
   * Collects {@link URLClassLoader} along the {@link ClassLoader} chain. This method recognizes both
   * {@link Delegator} and {@link CombineClassLoader} and will unfold the delegating classloaders inside it.
   * The order of insertion
   *
   * @param classLoader the classloader to start the search from
   * @param result a collection for storing the result
   * @param <T> type of the collection
   * @return the result collection
   */
  private static <T extends Collection<? super URLClassLoader>> T collectURLClassLoaders(ClassLoader classLoader,
                                                                                         T result) {
    // Do BFS from the bottom of the ClassLoader chain
    Deque<ClassLoader> queue = new LinkedList<>();
    queue.add(classLoader);
    while (!queue.isEmpty()) {
      ClassLoader cl = queue.remove();

      // Although CombineClassLoader is a URLClassLoader, we always get the delegates instead
      // This is for making sure we can get the parent ClassLoaders of each of the underlying delegate
      // for the search.
      if (cl instanceof CombineClassLoader) {
        List<ClassLoader> delegates = ((CombineClassLoader) cl).getDelegates();
        ListIterator<ClassLoader> iterator = delegates.listIterator(delegates.size());
        // Use add first for delegates, which effectively is replacing the current classloader
        while (iterator.hasPrevious()) {
          queue.addFirst(iterator.previous());
        }
      } else if (cl instanceof Delegator) {
        // Similarly for Delegator, although it might implement URLClassLoader, we get the delegate instead
        // so that the parent classloader can be correctly inspected later
        Object delegate = ((Delegator) cl).getDelegate();
        if (delegate != null && delegate instanceof ClassLoader) {
          // Use add first for delegate, which effectively is replacing the current classloader
          queue.addFirst((ClassLoader) delegate);
        }
      } else if (cl instanceof URLClassLoader) {
        result.add((URLClassLoader) cl);
      }

      if (cl.getParent() != null) {
        queue.add(cl.getParent());
      }
    }

    return result;
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

  /**
   * Extracts the Class-Path attributed from the given jar file and add those classpath urls.
   */
  private static <T extends Collection<? super URL>> void addClassPathFromJar(URL url, T urls) {
    try {
      File file = new File(url.toURI());
      try (JarFile jarFile = new JarFile(file)) {
        // Get the Class-Path attribute
        Manifest manifest = jarFile.getManifest();
        if (manifest == null) {
          return;
        }
        Object attr = manifest.getMainAttributes().get(Attributes.Name.CLASS_PATH);
        if (attr == null) {
          return;
        }

        // Add the URL
        for (String path : CLASS_PATH_ATTR_SPLITTER.split(attr.toString())) {
          URI uri = new URI(path);
          if (uri.isAbsolute()) {
            urls.add(uri.toURL());
          } else {
            urls.add(new File(file.getParentFile(), path.replace('/', File.separatorChar)).toURI().toURL());
          }
        }
      }
    } catch (Exception e) {
      // Ignore, as there can be jar file that doesn't exist on the FS.
    }
  }
}
