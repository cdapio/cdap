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

package co.cask.cdap.internal.asm;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * A {@link ClassLoader} for loading known bytecode.
 */
public class ByteCodeClassLoader extends ClassLoader {

  /**
   * Map from class name to bytecode.
   */
  protected final Map<String, byte[]> bytecodes;
  protected final Map<String, Class<?>> typeClasses;

  public ByteCodeClassLoader(ClassLoader parent) {
    super(parent);
    this.bytecodes = Maps.newHashMap();
    this.typeClasses = Maps.newHashMap();
  }

  /**
   * Adds a class definition for this ClassLoader. When the class as given in the definition is
   * loaded for the first time, the byte code inside the definition will be used to load the class.
   *
   * @param classDef The class definition
   */
  public final synchronized ByteCodeClassLoader addClass(ClassDefinition classDef) {
    bytecodes.put(classDef.getClassName(), classDef.getBytecode());
    for (Class<?> cls : classDef.getPreservedClasses()) {
      typeClasses.put(cls.getName(), cls);
    }
    return this;
  }

  @Override
  public Class<?> loadClass(String className) throws ClassNotFoundException {
    return loadClass(className, true);
  }

  @Override
  public synchronized Class<?> loadClass(String className, boolean resolveIt) throws ClassNotFoundException {

    Class<?> result = findLoadedClass(className);
    if (result != null) {
      return result;
    }

    // See if it is a known type. If yes, return it.
    result = typeClasses.get(className);
    if (result != null) {
      return result;
    }

    // Tries to load it from this classloader
    byte[] bytecode = bytecodes.get(className);
    if (bytecode != null) {
      result = defineClass(className, bytecode, 0, bytecode.length);
      if (resolveIt) {
        resolveClass(result);
      }
      return result;
    }

    // Load with parent classloader
    return super.loadClass(className, resolveIt);
  }
}
