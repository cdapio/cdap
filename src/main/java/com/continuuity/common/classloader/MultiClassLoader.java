package com.continuuity.common.classloader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Hashtable;

/**
 * A simple test class loader capable of loading from
 * multiple sources, such as local files or a URL.
 *
 * This class is derived from an article by Chuck McManis
 * http://www.javaworld.com/javaworld/jw-10-1996/indepth.src.html
 * with large modifications.
 */
public abstract class MultiClassLoader extends ClassLoader {
  private static final Logger Log = LoggerFactory.getLogger(MultiClassLoader.class);
  private Hashtable classes = new Hashtable();
  private char      classNameReplacementChar;

  protected boolean   monitorOn = false;
  protected boolean   sourceMonitorOn = true;

  public MultiClassLoader() { }

  /**
   * This is a simple version for external clients since they
   * will always want the class resolved before it is returned
   * to them.
   */
  public Class loadClass(String className) throws ClassNotFoundException {
    return (loadClass(className, true));
  }

  @SuppressWarnings("unchecked")
  public synchronized Class loadClass(String className,
                                      boolean resolveIt) throws ClassNotFoundException {

    Class   result;
    byte[]  classBytes;

    //Check our local cache of classes
    result = (Class)classes.get(className);
    if (result != null) {
      return result;
    }

    //Check with the primordial class loader
    try {
      result = super.findSystemClass(className);
      return result;
    } catch (ClassNotFoundException e) {
      Log.debug("System class '{}' loading error. Reason : {}.", className, e.getMessage());
    }

    //Try to load it from preferred source
    // Note loadClassBytes() is an abstract method
    classBytes = loadClassBytes(className);
    if (classBytes == null) {
      throw new ClassNotFoundException();
    }

    //Define it (parse the class file)
    result = defineClass(className, classBytes, 0, classBytes.length);
    if (result == null) {
      throw new ClassFormatError("Error parsing class " + className);
    }

    //Resolve if necessary
    if (resolveIt) resolveClass(result);

    // Done
    classes.put(className, result);
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
    if(className.contains("$")) {
      return className;
    }

    if (classNameReplacementChar == '\u0000') {
      // '/' is used to map the package to the path
      return className.replace('.', '/') + ".class";
    } else {
      // Replace '.' with custom char, such as '_'
      return className.replace('.',
          classNameReplacementChar) + ".class";
    }
  }

}
