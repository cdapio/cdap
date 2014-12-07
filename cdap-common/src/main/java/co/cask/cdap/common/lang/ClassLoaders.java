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

import com.google.common.base.Objects;

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
}
