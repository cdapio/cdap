/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.common.conf.CConfiguration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Helper class to load and instantiate class from external jar and get extension properties.
 */
public final class ExtensionClassHelper {

  private ExtensionClassHelper() {

  }

  /**
   * Inspect the given extension jar to find the extension class contained in it.
   *
   * @param extensionJar the bundled jar file for the extension
   * @param tmpDir temporary Directory
   * @param interfaceNameInExceptionMessage name of the interface that extensions implement
   * @param interfaceClassName name of the interface class that extensions implement
   * @return name of the class defined as the MAIN_CLASS in the extension jar
   * @throws IOException if there was an exception opening the jar file
   */
  private static String getExtensionClassName(File extensionJar, File tmpDir,
                                              String interfaceNameInExceptionMessage, String interfaceClassName)
    throws InvalidExtensionException, IOException {
    File manifestFile = new File(tmpDir, JarFile.MANIFEST_NAME);
    if (!manifestFile.isFile() && !manifestFile.exists()) {
      throw new InvalidExtensionException(
        String.format("No Manifest found in %s extension jar '%s'.", interfaceNameInExceptionMessage, extensionJar));
    }
    try (InputStream is = new FileInputStream(manifestFile)) {
      Manifest manifest = new Manifest(is);
      Attributes manifestAttributes = manifest.getMainAttributes();
      if (manifestAttributes == null) {
        throw new InvalidExtensionException(
          String.format("No attributes found in %s extension jar '%s'.", interfaceNameInExceptionMessage,
                        extensionJar));
      }
      if (!manifestAttributes.containsKey(Attributes.Name.MAIN_CLASS)) {
        throw new InvalidExtensionException(
          String.format("%s class not set in the manifest of the %s extension jar located at %s. " +
                          "Please set the attribute %s to the fully qualified class name of the class that " +
                          "implements %s in the extension jar's manifest.",
                        interfaceNameInExceptionMessage, interfaceNameInExceptionMessage,
                        extensionJar, Attributes.Name.MAIN_CLASS, interfaceClassName));
      }
      return manifestAttributes.getValue(Attributes.Name.MAIN_CLASS);
    }
  }

  /**
   * Checks if the extension jar exists and if its a valid file
   * @param extensionJar extension jar file
   * @param extensionDescription description used for interface extension implements in error messages
   * @param extensionPathProperty property used to specify the extension jar path
   * @throws InvalidExtensionException
   */
  public static void ensureValidExtensionJar(File extensionJar, String extensionDescription,
                                             String extensionPathProperty) throws InvalidExtensionException {
    if (!extensionJar.exists()) {
      throw new InvalidExtensionException(
        String.format("%s extension jar %s specified as %s does not exist.", extensionDescription, extensionJar,
                      extensionPathProperty)
      );
    }
    if (!extensionJar.isFile()) {
      throw new InvalidExtensionException(
        String.format("%s extension jar %s specified as %s must be a file.", extensionDescription, extensionJar,
                      extensionPathProperty)
      );
    }
  }

  /**
   * load and return the extension class
   * @param extensionJar path to extension jar file
   * @param extensionClassLoader extension class loader to use
   * @param assignableClass check if the loaded class is assignableFrom this class
   * @param tmpDir temporary dir
   * @param interfaceNameInExceptionMessages interface name that will be used in exception messages
   * @param interfaceClassName interface class name, extension implements this interface
   * @return loaded extension class
   * @throws IOException
   * @throws InvalidExtensionException
   */
  @SuppressWarnings("unchecked")
  public static Class loadExtensionClass(File extensionJar,
                                         ExtensionClassLoader extensionClassLoader,
                                         Class assignableClass,
                                         File tmpDir,
                                         String interfaceNameInExceptionMessages,
                                         String interfaceClassName)
    throws IOException, InvalidExtensionException {
    String extensionClassName = getExtensionClassName(extensionJar, tmpDir,
                                                      interfaceNameInExceptionMessages, interfaceClassName);
    Class<?> extensionClass;
    try {
      extensionClass = extensionClassLoader.loadClass(extensionClassName);
    } catch (ClassNotFoundException e) {
      throw new InvalidExtensionException(
        String.format("%s extension class %s not found. Please make sure that the right class is specified " +
                        "in the extension jar's manifest located at %s.",
                      interfaceNameInExceptionMessages, extensionClassName, extensionJar), e);
    }
    if (!assignableClass.isAssignableFrom(extensionClass)) {
      throw new InvalidExtensionException(
        String.format("Class %s defined as %s in the %s extension's manifest at %s must implement %s",
                      extensionClass.getName(), Attributes.Name.MAIN_CLASS,
                      interfaceNameInExceptionMessages, extensionJar,
                      interfaceClassName));
    }
    return extensionClass;
  }

  /**
   * populate and return properties for externsion, specified fromm configuration.
   * @param cConf configuration
   * @param extensionPrefix prefix used for the extension properties
   * @return {@link Properties}
   */
  public static Properties createExtensionProperties(CConfiguration cConf, String extensionPrefix) {
    Properties extensionProperties = new Properties();
    for (Map.Entry<String, String> cConfEntry : cConf) {
      if (cConfEntry.getKey().startsWith(extensionPrefix)) {
        extensionProperties.put(
          cConfEntry.getKey().substring(extensionPrefix.length()), cConfEntry.getValue()
        );
      }
    }
    return extensionProperties;
  }
}
