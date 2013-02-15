/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.archive;

import javax.annotation.Nullable;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * JarClassLoader extends {@link MultiClassLoader}
 */
public class JarClassLoader extends MultiClassLoader {
  private JarResources jarResources;

  /**
   * Create the JarResource and suck in the archive file.
   *
   * @param jarName Name of the archive file.
   */
  public JarClassLoader(String jarName) throws JarResourceException {
    jarResources = new JarResources (jarName);
  }

  /**
   * Creates an instance of archive with provided archive resources.
   *
   * @param jarResources instance of archive resources
   * @throws JarResourceException
   */
  public JarClassLoader(JarResources jarResources) throws JarResourceException {
    this.jarResources = jarResources;
  }

  /**
   * Loads the class bytes based on the name specified. Name
   * munging is used to identify the class to be loaded from
   * the archive.
   *
   * @param className Name of the class bytes to be loaded.
   * @return array of bytes for the class.
   */
  @Nullable
  public byte[] loadClassBytes (String className) {
    className = formatClassName (className);
    return (jarResources.getResource (className));
  }

  /**
   * Reads the class name from Main-Class attribute in the MANIFEST file and returns an instance
   * of the class if class type matches <code>expectedClass</code>
   *
   * @param expectedClass to be loaded from the MANIFEST as Main-Class
   * @return Instance of class specified in MAINFEST file Main-Class.
   * @throws IllegalStateException in case of errors in manifest.
   * @throws ClassNotFoundException If there are any issues loading the class.
   * @throws IllegalAccessException If the class to be loaded cannot be accessed.
   * @throws InstantiationException If there are issue instantiating the class specified in main-class.
   */
  @Nullable
  public Object getMainClass(Class<?> expectedClass) throws IllegalStateException, ClassNotFoundException,
    IllegalAccessException, InstantiationException {

    // Get MANIFEST file and extract the Main-Class from it.
    Manifest manifest = jarResources.getManifest();

    if(manifest == null) {
      throw new IllegalStateException("MANIFEST file does not exist.");
    }

    // Extract the Main-Class from the manifest file.
    Attributes attributes = manifest.getMainAttributes();
    if(! attributes.containsKey(Attributes.Name.MAIN_CLASS)) {
      throw new IllegalStateException("MANIFEST exists, but " + Attributes.Name.MAIN_CLASS.toString()
                                        + " attribute is missing.");
    }

    // Attempt to read the main class name.
    String className = attributes.getValue(Attributes.Name.MAIN_CLASS);
    if(className == null || className.isEmpty()) {
      throw new IllegalStateException("Class name in MANIFEST is emtpy");
    }

    Class<?> loadedClass = loadClass(className);
    if(! loadedClass.isAssignableFrom(expectedClass)) {
      throw new IllegalStateException("Found " + loadedClass.getCanonicalName() + ", Expected " +
        expectedClass.getCanonicalName());
    }
    return loadedClass.newInstance();
  }
}
