/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.lang;

import javax.annotation.Nullable;

/**
 * Utility class for collection of methods for deal with ClassLoader and loading class.
 */
public final class ClassLoaders {

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
    ClassLoader cl = classLoader;
    if (cl == null) {
      cl = Thread.currentThread().getContextClassLoader();
      if (cl == null) {
        cl = caller.getClass().getClassLoader();
      }
    }

    return cl.loadClass(className);
  }

  private ClassLoaders() {
  }
}
