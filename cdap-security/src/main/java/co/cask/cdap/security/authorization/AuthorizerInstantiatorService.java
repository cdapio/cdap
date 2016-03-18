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

package co.cask.cdap.security.authorization;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.NoOpAuthorizer;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.zip.ZipException;

/**
 * Class to instantiate {@link Authorizer} extensions. {@link Authorizer} extensions are instantiated using a
 * separate {@link ClassLoader} that is built using a bundled jar for the {@link Authorizer} implementation that
 * contains all its required dependencies. The {@link ClassLoader} is created with the parent as the classloader with
 * which the {@link Authorizer} interface is instantiated. This parent only has classes required by the
 * {@code cdap-security-spi} module.
 *
 * The {@link AuthorizerInstantiatorService} has the following expectations from the extension:
 * <ul>
 *   <li>Authorization is enabled setting the parameter {@link Constants.Security.Authorization#ENABLED} to true in
 *   {@code cdap-site.xml}. When authorization is disabled, an instance of {@link NoOpAuthorizer} is returned.</li>
 *   <li>The path to the extension jar bundled with all its dependencies is read from the setting
 *   {@link Constants.Security.Authorization#EXTENSION_JAR_PATH} in cdap-site.xml</li>
 *   <li>The instantiator reads a fully qualified class name specified as the {@link Attributes.Name#MAIN_CLASS}
 *   attribute in the extension jar's manifest file. This class must implement {@link Authorizer} and have a public
 *   constructor that accepts a single {@link Properties} object as parameter. This constructor is invoked with a
 *   {@link Properties} object that is populated with all configuration settings from {@code cdap-site.xml} that have
 *   keys with the prefix {@link Constants.Security.Authorization#EXTENSION_CONFIG_PREFIX}.</li>
 * </ul>
 *
 * Implemented as a {@link AbstractIdleService} so it can manage initialization and destruction of the
 * {@link AuthorizerClassLoader} in a reliable manner.
 */
public class AuthorizerInstantiatorService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(AuthorizerInstantiatorService.class);

  private final CConfiguration cConf;
  private final boolean authorizationEnabled;

  private File tmpDir;
  private AuthorizerClassLoader authorizerClassLoader;
  private Authorizer authorizer;

  @Inject
  public AuthorizerInstantiatorService(CConfiguration cConf) {
    this.cConf = cConf;
    this.authorizationEnabled = cConf.getBoolean(Constants.Security.Authorization.ENABLED);
  }

  @Override
  protected void startUp() throws Exception {
    if (!authorizationEnabled) {
      this.authorizer = new NoOpAuthorizer();
      return;
    }
    // Authorization is enabled, so continue with startup now
    String authorizerExtensionJarPath = cConf.get(Constants.Security.Authorization.EXTENSION_JAR_PATH);
    if (Strings.isNullOrEmpty(authorizerExtensionJarPath)) {
      throw new IllegalArgumentException(
        String.format("Authorizer extension jar path not found in configuration. Please set %s in cdap-site.xml to " +
                        "the fully qualified path of the jar file to use as the authorization backend.",
                      Constants.Security.Authorization.EXTENSION_JAR_PATH));
    }
    File authorizerExtensionJar = new File(authorizerExtensionJarPath);
    ensureValidAuthExtensionJar(authorizerExtensionJar);
    File tmpDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                           cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    this.tmpDir = DirUtils.createTempDir(tmpDir);
    this.authorizerClassLoader = createAuthorizerClassLoader(authorizerExtensionJar);
    this.authorizer = createAuthorizerInstance(authorizerExtensionJar);
  }

  @Override
  protected void shutDown() throws Exception {
    if (!authorizationEnabled) {
      // nothing to close, since we would not have created a class loader
      return;
    }
    try {
      authorizerClassLoader.close();
    } catch (IOException e) {
      LOG.warn("Failed to close authorizer class loader", e);
    }
    try {
      DirUtils.deleteDirectoryContents(tmpDir);
    } catch (IOException e) {
      // It's a cleanup step. Nothing much can be done if cleanup fails.
      LOG.warn("Failed to delete directory {}", tmpDir, e);
    }
  }

  /**
   * @return an instance of the configured {@link Authorizer} extension, or of {@link NoOpAuthorizer}, if authorization
   * is disabled
   */
  public Authorizer get() throws IOException, InvalidAuthorizerException {
    return authorizer;
  }

  /**
   * Creates a new instance of the configured {@link Authorizer} extension, based on the provided extension jar file.
   *
   * @return a new instance of the configured {@link Authorizer} extension
   */
  private Authorizer createAuthorizerInstance(File authorizerExtensionJar)
    throws IOException, InvalidAuthorizerException {
    Class<? extends Authorizer> authorizerClass = loadAuthorizerClass(authorizerExtensionJar);
    // Set the context class loader to the AuthorizerClassLoader before creating a new instance of the extension,
    // so all classes required in this process are created from the AuthorizerClassLoader.
    ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(authorizerClassLoader);
    LOG.debug("Setting context classloader to {}. Old classloader was {}.", authorizerClassLoader, oldClassLoader);
    try {
      Constructor<? extends Authorizer> constructor;
      try {
        constructor = authorizerClass.getDeclaredConstructor(Properties.class);
      } catch (NoSuchMethodException e) {
        throw new InvalidAuthorizerException(
          String.format("No suitable constructor for Authorizer extension %s. Please make sure that the extension is " +
                          "a public class with a public constructor that accepts a single parameter of type %s.",
                        authorizerClass.getName(), Properties.class.getName()), e);
      }
      try {
        return constructor.newInstance(createExtensionProperties());
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new InvalidAuthorizerException(
          String.format("Error while instantiating for authorizer extension %s. Please make sure that the extension " +
                          "is a public class with a public constructor that accepts a single parameter of type %s.",
                        authorizerClass.getName(), Properties.class.getName()), e);
      }
    } finally {
      // After the process of creation of a new instance has completed (success or failure), reset the context
      // classloader back to the original class loader.
      Thread.currentThread().setContextClassLoader(oldClassLoader);
      LOG.debug("Resetting context classloader to {} from {}.", oldClassLoader, authorizerClassLoader);
    }
  }

  private Properties createExtensionProperties() {
    Properties extensionProperties = new Properties();
    for (Map.Entry<String, String> cConfEntry : cConf) {
      if (cConfEntry.getKey().startsWith(Constants.Security.Authorization.EXTENSION_CONFIG_PREFIX)) {
        extensionProperties.put(
          cConfEntry.getKey().substring(Constants.Security.Authorization.EXTENSION_CONFIG_PREFIX.length()),
          cConfEntry.getValue()
        );
      }
    }
    return extensionProperties;
  }

  private AuthorizerClassLoader createAuthorizerClassLoader(File authorizerExtensionJar)
    throws IOException, InvalidAuthorizerException {
    LOG.info("Creating authorization extension using jar {}.", authorizerExtensionJar);
    try {
      BundleJarUtil.unJar(Locations.toLocation(authorizerExtensionJar), tmpDir);
      return new AuthorizerClassLoader(tmpDir);
    } catch (ZipException e) {
      throw new InvalidAuthorizerException(
        String.format("Authorization extension jar %s specified as %s must be a jar file.", authorizerExtensionJar,
                      Constants.Security.Authorization.EXTENSION_JAR_PATH), e
      );
    }
  }

  @SuppressWarnings("unchecked")
  private Class<? extends Authorizer> loadAuthorizerClass(File authorizerExtensionJar)
    throws IOException, InvalidAuthorizerException {
    String authorizerClassName = getAuthorizerClassName(authorizerExtensionJar);
    Class<?> authorizerClass;
    try {
      authorizerClass = authorizerClassLoader.loadClass(authorizerClassName);
    } catch (ClassNotFoundException e) {
      throw new InvalidAuthorizerException(
        String.format("Authorizer extension class %s not found. Please make sure that the right class is specified " +
                        "in the extension jar's manifest located at %s.",
                      authorizerClassName, authorizerExtensionJar), e);
    }
    if (!Authorizer.class.isAssignableFrom(authorizerClass)) {
      throw new InvalidAuthorizerException(
        String.format("Class %s defined as %s in the authorization extension's manifest at %s must implement %s",
                      authorizerClass.getName(), Attributes.Name.MAIN_CLASS, authorizerExtensionJar,
                      Authorizer.class.getName()));
    }
    return (Class<? extends Authorizer>) authorizerClass;
  }

  /**
   * Inspect the given auth extension jar to find the {@link Authorizer} class contained in it.
   *
   * @param authorizerExtensionJar the bundled jar file for the authorizer extension
   * @return name of the class defined as the {@link Attributes.Name#MAIN_CLASS} in the authorizer extension jar
   * @throws IOException if there was an exception opening the jar file
   */
  private String getAuthorizerClassName(File authorizerExtensionJar) throws IOException, InvalidAuthorizerException {
    File manifestFile = new File(tmpDir, JarFile.MANIFEST_NAME);
    if (!manifestFile.isFile() && !manifestFile.exists()) {
      throw new InvalidAuthorizerException(
        String.format("No Manifest found in authorizer extension jar '%s'.", authorizerExtensionJar));
    }
    try (InputStream is = new FileInputStream(manifestFile)) {
      Manifest manifest = new Manifest(is);
      Attributes manifestAttributes = manifest.getMainAttributes();
      if (manifestAttributes == null) {
        throw new InvalidAuthorizerException(
          String.format("No attributes found in authorizer extension jar '%s'.", authorizerExtensionJar));
      }
      if (!manifestAttributes.containsKey(Attributes.Name.MAIN_CLASS)) {
        throw new InvalidAuthorizerException(
          String.format("Authorizer class not set in the manifest of the authorizer extension jar located at %s. " +
                          "Please set the attribute %s to the fully qualified class name of the class that " +
                          "implements %s in the extension jar's manifest.",
                        authorizerExtensionJar, Attributes.Name.MAIN_CLASS, Authorizer.class.getName()));
      }
      return manifestAttributes.getValue(Attributes.Name.MAIN_CLASS);
    }
  }

  private void ensureValidAuthExtensionJar(File authorizerExtensionJar) throws InvalidAuthorizerException {
    if (!authorizerExtensionJar.exists()) {
      throw new InvalidAuthorizerException(
        String.format("Authorization extension jar %s specified as %s does not exist.", authorizerExtensionJar,
                      Constants.Security.Authorization.EXTENSION_JAR_PATH)
      );
    }
    if (!authorizerExtensionJar.isFile()) {
      throw new InvalidAuthorizerException(
        String.format("Authorization extension jar %s specified as %s must be a file.", authorizerExtensionJar,
                      Constants.Security.Authorization.EXTENSION_JAR_PATH)
      );
    }
  }
}
