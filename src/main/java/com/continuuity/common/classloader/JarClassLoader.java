package com.continuuity.common.classloader;

/**
 * JarClassLoader extends {@link MultiClassLoader}
 */
public class JarClassLoader extends MultiClassLoader {
  private JarResources    jarResources;

  /**
   * Create the JarResource and suck in the jar file.
   *
   * @param jarName Name of the jar file.
   */
  public JarClassLoader (String jarName) throws JarResourceException {
    jarResources = new JarResources (jarName);
  }

  /**
   * Creates an instance of classloader with provided jar resources.
   *
   * @param jarResources instance of jar resources
   * @throws JarResourceException
   */
  public JarClassLoader(JarResources jarResources) throws JarResourceException {
    this.jarResources = jarResources;
  }

  /**
   * Loads the class bytes based on the name specified. Name
   * munging is used to identify the class to be loaded from
   * the jar.
   *
   * @param className Name of the class bytes to be loaded.
   * @return array of bytes for the class.
   */
  protected byte[] loadClassBytes (String className) {
    className = formatClassName (className);
    return (jarResources.getResource (className));
  }
}
