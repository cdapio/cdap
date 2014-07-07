package com.continuuity.internal.asm;

import com.google.common.collect.Maps;

import java.util.Map;
import javax.annotation.Nullable;

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
   * @param typeClass An optional class that the given class definition is generated from. This is to make sure
   *                  the type class is always loaded by the original ClassLoader when resolving the generated class
   *                  as given by the class definition.
   */
  public final synchronized ByteCodeClassLoader addClass(ClassDefinition classDef,
                                                         @Nullable Class<?> typeClass) {
    bytecodes.put(classDef.getClassName(), classDef.getBytecode());
    if (typeClass != null) {
      typeClasses.put(typeClass.getName(), typeClass);
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

    //Check with the parent classloader
    ClassLoader parent = getParent();
    if (parent != null) {
      return parent.loadClass(className);
    }

    throw new ClassNotFoundException("Failed to load class: " + className);
  }
}
