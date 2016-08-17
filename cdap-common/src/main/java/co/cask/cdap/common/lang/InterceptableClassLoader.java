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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * A {@link URLClassLoader} that can optionally rewrite the the bytecode.
 */
public abstract class InterceptableClassLoader extends URLClassLoader implements ClassRewriter {

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
}
