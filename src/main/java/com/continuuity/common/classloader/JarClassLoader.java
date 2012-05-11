package com.continuuity.common.classloader;

/**
 * JarClassLoader extends {@link MultiClassLoader}
 */

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class JarClassLoader extends MultiClassLoader {
  private JarResources    jarResources;

  /**
   * Create the JarResource and suck in the jar file.
   * @param jarName Name of the jar file.
   */
  public JarClassLoader (String jarName) {
    jarResources = new JarResources (jarName);
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
