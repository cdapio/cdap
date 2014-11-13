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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Queue;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Utility class for collection of methods for dealing with ClassLoader and loading class.
 */
public final class ClassLoaders {

  private ClassLoaders() { }

  /**
   * Returns the ClassLoader of the given type. If the given type is a {@link ParameterizedType}, it returns
   * a {@link CombineClassLoader} of all types. The context ClassLoader or System ClassLoader would be used as
   * the parent of the CombineClassLoader.
   *
   * @return A new CombineClassLoader. If no ClassLoader is found from the type,
   *         it returns the current thread context ClassLoader if it's not null, otherwise, return system ClassLoader.
   */
  public static ClassLoader getClassLoader(TypeToken<?> type) {
    Set<ClassLoader> classLoaders = Sets.newIdentityHashSet();

    // Breath first traversal into the Type.
    Queue<TypeToken<?>> queue = Lists.newLinkedList();
    queue.add(type);
    while (!queue.isEmpty()) {
      type = queue.remove();
      ClassLoader classLoader = type.getRawType().getClassLoader();
      if (classLoader != null) {
        classLoaders.add(classLoader);
      }

      if (type.getType() instanceof ParameterizedType) {
        for (Type typeArg : ((ParameterizedType) type.getType()).getActualTypeArguments()) {
          queue.add(TypeToken.of(typeArg));
        }
      }
    }

    // Determine the parent classloader
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    ClassLoader parent = (contextClassLoader == null) ? ClassLoader.getSystemClassLoader() : contextClassLoader;

    if (classLoaders.isEmpty()) {
      return parent;
    }
    return new CombineClassLoader(parent, classLoaders);
  }

  /**
   * Loads the class with the given class name with the given classloader. If it is {@code null},
   * load the class with the context ClassLoader of current thread if it presents, otherwise load the class
   * with the ClassLoader of the caller object.
   *
   * @param className Name of the class to load.
   * @param classLoader Classloader for loading the class. It could be {@code null}.
   * @param caller The object who call this method.
   * @return The loaded class.
   * @throws ClassNotFoundException If failed to load the given class.
   */
  public static Class<?> loadClass(String className, @Nullable ClassLoader classLoader,
                                   Object caller) throws ClassNotFoundException {
    ClassLoader cl = Objects.firstNonNull(classLoader,
                                          Objects.firstNonNull(Thread.currentThread().getContextClassLoader(),
                                                               caller.getClass().getClassLoader()));
    return cl.loadClass(className);
  }
}
