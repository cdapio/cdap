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

package co.cask.cdap.data.runtime.main;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import javax.annotation.Nullable;

/**
 * A classloader that loads resources from a given URL list, or from parent classloader if the URL list
 * does not contain the resource. Classes and other objects are loaded from its parent classloader.
 */
public class ResourcesClassLoader extends URLClassLoader {
  private static final Logger LOG = LoggerFactory.getLogger(ResourcesClassLoader.class);

  private final URLClassLoader resourceClassLoader;

  public ResourcesClassLoader(URL[] resourceUrls, ClassLoader parent) {
    super(new URL[0], parent);
    this.resourceClassLoader = new URLClassLoader(resourceUrls, ClassLoader.getSystemClassLoader().getParent());
  }

  @Override
  public URL getResource(String name) {
    URL resource = firstNonNull(resourceClassLoader.getResource(name), super.getResource(name));
    LOG.trace("Returning {} for {}", resource, name);
    return resource;
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    Enumeration<URL> resources = firstNonNull(resourceClassLoader.getResources(name), super.getResources(name));
    LOG.trace("Returning {} for {}", resources, name);
    return resources;
  }

  @Override
  public InputStream getResourceAsStream(String name) {
    return firstNonNull(resourceClassLoader.getResourceAsStream(name), super.getResourceAsStream(name));
  }

  @VisibleForTesting
  @Nullable
  static <T> T firstNonNull(@Nullable T first, @Nullable T second) {
    return first == null ? second : first;
  }
}
