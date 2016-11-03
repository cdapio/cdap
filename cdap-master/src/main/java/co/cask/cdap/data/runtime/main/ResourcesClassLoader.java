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

  public ResourcesClassLoader(URL[] resourceUrls, ClassLoader parent) {
    super(resourceUrls, parent);
  }

  @Override
  public URL getResource(String name) {
    // Instead of following normal Classloader delegation, we try to find the resource from this classloader first
    return firstNonNull(findResource(name), super.getResource(name));
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    Enumeration<URL> resources = findResources(name);
    // If the resources is empty, use the normal classloader resource resolution
    if (resources.hasMoreElements()) {
      return resources;
    }
    return super.getResources(name);
  }

  @Override
  public InputStream getResourceAsStream(String name) {
    URL resource = getResource(name);
    try {
      return resource == null ? null : resource.openStream();
    } catch (Exception e) {
      // According to the javadoc, if the caller doesn't have the privilege opening the stream, null will be returned.
      return null;
    }
  }

  @VisibleForTesting
  @Nullable
  static <T> T firstNonNull(@Nullable T first, @Nullable T second) {
    return first == null ? second : first;
  }
}
