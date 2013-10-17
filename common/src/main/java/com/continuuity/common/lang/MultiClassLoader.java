/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.common.lang;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple test class loader capable of loading from
 * multiple sources, such as local files or a URL.
 * <p/>
 * This class is derived from an article by Chuck McManis
 * http://www.javaworld.com/javaworld/jw-10-1996/indepth.src.html
 * with large modifications.
 */
public abstract class MultiClassLoader extends ClassLoader {
  private static final Logger LOG = LoggerFactory.getLogger(MultiClassLoader.class);

  // Sets of class prefix that would not be loaded by the class loader
  private static final String[] CLASS_PREFER_EXEMPTIONS = new String[] {
    // Java standard library:
    "com.sun.",
    "launcher.",
    "java.",
    "javax.",
    "org.ietf",
    "org.omg",
    "org.w3c",
    "org.xml",
    "sunw.",
    // logging
    "org.slf4j",
    "com.hadoop",
    // Hadoop/HBase/ZK:
    "org.apache.hadoop",
    "org.apache.zookeeper",
    // Continuuity
    "com.continuuity",
    // Guava
    "com.google.common",
  };

  private char classNameReplacementChar;

  /**
   * Creates a ClassLoader with the given ClassLoader as its parent.
   * @param parent The parent ClassLoader
   */
  protected MultiClassLoader(ClassLoader parent) {
    super(parent);
  }

  /**
   * This is a simple version for external clients since they
   * will always want the class resolved before it is returned
   * to them.
   */
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

    //Try to load it from preferred source
    // Note loadClassBytes() is an abstract method
    boolean preferred = isPreferred(className);
    byte[] classBytes = preferred ? loadClassBytes(className) : null;
    if (classBytes == null) {
      //Check with the parent classloader
      try {
        ClassLoader parent = getParent();
        try {
          if (parent != null) {
            return parent.loadClass(className);
          }
        } catch (ClassNotFoundException e) {
          return ClassLoader.getSystemClassLoader().loadClass(className);
        }
        return ClassLoader.getSystemClassLoader().loadClass(className);
      } catch (ClassNotFoundException e) {
        if (!preferred) {
          // Tries to load it from this classloader
          classBytes = loadClassBytes(className);
          if (classBytes == null) {
            LOG.trace("Fail to load class {}", className);
            throw e;
          }
        } else {
          LOG.trace("System class '{}' loading error. Reason : {}.", className, e.getMessage());
          throw e;
        }
      }
    }

    //Define it (parse the class file)
    result = defineClass(className, classBytes, 0, classBytes.length);
    if (result == null) {
      throw new ClassFormatError("Error parsing class " + className);
    }

    //Resolve if necessary
    if (resolveIt) {
      resolveClass(result);
    }

    return result;
  }

  /**
   * This optional call allows a class name such as
   * "COM.test.Hello" to be changed to "COM_test_Hello",
   * which is useful for storing classes from different
   * packages in the same retrival directory.
   * In the above example the char would be '_'.
   */
  public void setClassNameReplacementChar(char replacement) {
    classNameReplacementChar = replacement;
  }

  protected abstract byte[] loadClassBytes(String className);

  protected String formatClassName(String className) {
    if (classNameReplacementChar == '\u0000') {
      // '/' is used to map the package to the path
      return className.replace('.', '/') + ".class";
    } else {
      // Replace '.' with custom char, such as '_'
      return className.replace('.', classNameReplacementChar) + ".class";
    }
  }

  private boolean isPreferred(String className) {
    for (String prefix : CLASS_PREFER_EXEMPTIONS) {
      if (className.startsWith(prefix)) {
        return false;
      }
    }
    return true;
  }

}
