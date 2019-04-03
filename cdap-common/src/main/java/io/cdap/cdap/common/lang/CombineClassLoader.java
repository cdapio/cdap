/*
 * Copyright © 2015 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A {@link ClassLoader} that load classes from list of other {@link ClassLoader}s. Note that
 * this ClassLoader just delegates to other ClassLoaders, but never define class, hence no Class
 * loaded by this class would have {@link Class#getClassLoader()}} returning this ClassLoader.
 */
public class CombineClassLoader extends URLClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(CombineClassLoader.class);
  private final List<ClassLoader> delegates;

  /**
   * Creates a CombineClassLoader with the given parent and a list of ClassLoaders for delegation.
   *
   * @param parent parent ClassLoader. If null, bootstrap ClassLoader will be the parent.
   * @param delegates list of ClassLoaders for delegation
   */
  public CombineClassLoader(@Nullable ClassLoader parent, ClassLoader...delegates) {
    this(parent, Arrays.asList(delegates));
  }

  /**
   * Creates a CombineClassLoader with the given parent and a list of ClassLoaders for delegation.
   *
   * @param parent parent ClassLoader. If null, bootstrap ClassLoader will be the parent.
   * @param delegates list of ClassLoaders for delegation
   */
  public CombineClassLoader(@Nullable ClassLoader parent, Iterable<? extends ClassLoader> delegates) {
    super(new URL[0], parent);
    this.delegates = ImmutableList.copyOf(delegates);
  }

  @Override
  public URL[] getURLs() {
    List<URL> urls = new ArrayList<>();
    for (ClassLoader delegate : delegates) {
      if (delegate instanceof URLClassLoader) {
        Collections.addAll(urls, ((URLClassLoader) delegate).getURLs());
      }
    }
    return urls.toArray(new URL[urls.size()]);
  }

  /**
   * Returns the list of {@link ClassLoader}s that this class delegates to.
   */
  public List<ClassLoader> getDelegates() {
    return delegates;
  }

  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    for (ClassLoader classLoader : delegates) {
      try {
        return classLoader.loadClass(name);
      } catch (ClassNotFoundException e) {
        LOG.trace("Class {} not found in ClassLoader {}", name, classLoader);
      }
    }

    throw new ClassNotFoundException("Class not found in all delegated ClassLoaders: " + name);
  }

  @Override
  public URL findResource(String name) {
    for (ClassLoader classLoader : delegates) {
      URL url = classLoader.getResource(name);
      if (url != null) {
        return url;
      }
    }
    return null;
  }

  @Override
  public Enumeration<URL> findResources(String name) throws IOException {
    // Using LinkedHashSet to preserve the ordering
    Set<URL> urls = Sets.newLinkedHashSet();
    for (ClassLoader classLoader : delegates) {
      Iterators.addAll(urls, Iterators.forEnumeration(classLoader.getResources(name)));
    }
    return Iterators.asEnumeration(urls.iterator());
  }

  @Override
  public InputStream getResourceAsStream(String name) {
    for (ClassLoader classLoader : delegates) {
      InputStream is = classLoader.getResourceAsStream(name);
      if (is != null) {
        return is;
      }
    }
    return null;
  }
}
