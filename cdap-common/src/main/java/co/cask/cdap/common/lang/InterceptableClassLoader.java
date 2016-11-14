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

package co.cask.cdap.common.lang;

import co.cask.cdap.common.lang.jar.BundleJarUtil;
import com.google.common.io.InputSupplier;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 * A {@link URLClassLoader} that can optionally rewrite the the bytecode.
 */
public abstract class InterceptableClassLoader extends URLClassLoader implements ClassRewriter {

  private final Map<String, Manifest> manifests = new HashMap<>();

  public InterceptableClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
  }

  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    if (!needIntercept(name)) {
      return super.findClass(name);
    }

    // Use findResource instead of getResource the class being intercepted is supposed to have this
    // classloader as the defining classloader, not the parent one
    URL resource = findResource(name.replace('.', '/') + ".class");
    if (resource == null) {
      throw new ClassNotFoundException("Failed to find resource for class " + name);
    }
    try (InputStream is = resource.openStream()) {
      byte[] bytecode = rewriteClass(name, is);

      // Define the package based on the class package name
      String packageName = getPackageName(name);
      if (packageName != null && getPackage(packageName) == null) {
        Manifest manifest = getManifest(resource);
        if (manifest == null) {
          definePackage(packageName, null, null, null, null, null, null, null);
        } else {
          definePackage(packageName, manifest, resource);
        }
      }
      return defineClass(name, bytecode, 0, bytecode.length);
    } catch (IOException e) {
      throw new ClassNotFoundException("Failed to read class definition for class " + name, e);
    }
  }

  /**
   * Implementation to decide whether a class loading needs to be intercepted by this class.
   *
   * @return {@code true} to have the class loading intercepted; {@code false} otherwise
   */
  protected abstract boolean needIntercept(String className);

  /**
   * Returns the package name of the given class name or return {@code null} if the given class is in default package.
   */
  @Nullable
  private String getPackageName(String className) {
    int idx = className.lastIndexOf('.');
    return idx >= 0 ? className.substring(0, idx) : null;
  }

  /**
   * Returns the {@link Manifest} of the given resource if it is representing a local JAR file.
   */
  @Nullable
  private Manifest getManifest(URL resource) {
    if (!"jar".equals(resource.getProtocol())) {
      return null;
    }

    String path = resource.getFile();
    final String jarURIString = path.substring(0, path.indexOf("!/"));

    // This synchronized block shouldn't be adding overhead unless we enable this classloader to be parallel capable
    // (which we don't right now). For non-parallel capable classloader, an object lock was already acquired in
    // the loadClass call (caller of this method).
    synchronized (this) {
      if (!manifests.containsKey(jarURIString)) {
        try {
          // Tries to load the Manifest from the Jar URI
          final URI jarURI = URI.create(jarURIString);
          manifests.put(jarURIString, BundleJarUtil.getManifest(jarURI, new InputSupplier<InputStream>() {
            @Override
            public InputStream getInput() throws IOException {
              return jarURI.toURL().openStream();
            }
          }));
        } catch (IOException e) {
          // Ignore if cannot get Manifest from the jar file and remember the failure
          manifests.put(jarURIString, null);
        }
      }
      return manifests.get(jarURIString);
    }
  }
}
