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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.net.URL;
import java.util.Enumeration;

/**
 * A ClassLoader that do class loading by delegating to another ClassLoader. It holds the delegating ClassLoader
 * with a {@link WeakReference} so that garbage collection of the delegating ClassLoader is possible.
 */
public class WeakReferenceDelegatorClassLoader extends ClassLoader implements Delegator<ClassLoader> {

  private static final Logger LOG = LoggerFactory.getLogger(WeakReferenceDelegatorClassLoader.class);

  private final WeakReference<ClassLoader> delegate;

  public WeakReferenceDelegatorClassLoader(ClassLoader classLoader) {
    // Parent is null, meaning it's the bootstrap ClassLoader
    super(null);
    this.delegate = new WeakReference<>(classLoader);
  }

  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    return ensureDelegateExists().loadClass(name);
  }

  @Override
  public URL getResource(String name) {
    return ensureDelegateExists().getResource(name);
  }

  @Override
  public InputStream getResourceAsStream(String name) {
    return ensureDelegateExists().getResourceAsStream(name);
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    return ensureDelegateExists().getResources(name);
  }

  @Override
  public ClassLoader getDelegate() {
    return delegate.get();
  }

  private ClassLoader ensureDelegateExists() {
    ClassLoader classLoader = delegate.get();
    if (classLoader == null) {
      classLoader = getClass().getClassLoader();
      LOG.warn("Delegating ClassLoader is already Garbage Collected. Using system ClassLoader instead: " + classLoader);
    }
    return classLoader;
  }
}
