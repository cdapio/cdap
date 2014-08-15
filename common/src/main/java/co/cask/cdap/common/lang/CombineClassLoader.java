/*
 * Copyright 2014 Cask, Inc.
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

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Set;

/**
 * A {@link ClassLoader} that load classes from list of other {@link ClassLoader}s. Node that
 * this ClassLoader just delegates to other ClassLoaders, but never define class, hence no Class
 * loaded by this class would have {@link Class#getClassLoader()}} returning this ClassLoader.
 */
public final class CombineClassLoader extends ClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(CombineClassLoader.class);
  private final Iterable<ClassLoader> delegates;

  public CombineClassLoader(ClassLoader parent, Iterable<ClassLoader> delegates) {
    super(parent);
    this.delegates = delegates;
  }

  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    Iterator<ClassLoader> iterator = delegates.iterator();
    while (iterator.hasNext()) {
      ClassLoader classLoader = iterator.next();
      try {
        return classLoader.loadClass(name);
      } catch (ClassNotFoundException e) {
        LOG.trace("Class {} not found in ClassLoader {}", name, classLoader);
      }
    }

    throw new ClassNotFoundException("Class not found in all delegated ClassLoaders: " + name);
  }

  @Override
  protected URL findResource(String name) {
    for (ClassLoader classLoader : delegates) {
      URL url = classLoader.getResource(name);
      if (url != null) {
        return url;
      }
    }
    return null;
  }

  @Override
  protected Enumeration<URL> findResources(String name) throws IOException {
    Set<URL> urls = Sets.newHashSet();
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
