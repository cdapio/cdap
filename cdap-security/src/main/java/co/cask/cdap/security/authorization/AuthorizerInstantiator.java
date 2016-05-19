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
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.security.spi.authorization.AuthorizationContext;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.NoOpAuthorizer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.zip.ZipException;

/**
 * Class to instantiate {@link Authorizer} extensions. Authorization extensions are instantiated using a
 * separate {@link ClassLoader} that is built using a bundled jar for the {@link Authorizer} extension that
 * contains all its required dependencies. The {@link ClassLoader} is created with the parent as the classloader with
 * which the {@link Authorizer} interface is instantiated. This parent only has classes required by the
 * {@code cdap-security-spi} module.
 *
 * The {@link AuthorizerInstantiator} has the following expectations from the extension:
 * <ul>
 *   <li>Authorization is enabled setting the parameter {@link Constants.Security.Authorization#ENABLED} to true in
 *   {@code cdap-site.xml}. When authorization is disabled, an instance of {@link NoOpAuthorizer} is returned.</li>
 *   <li>The path to the extension jar bundled with all its dependencies is read from the setting
 *   {@link Constants.Security.Authorization#EXTENSION_JAR_PATH} in cdap-site.xml</li>
 *   <li>The instantiator reads a fully qualified class name specified as the {@link Attributes.Name#MAIN_CLASS}
 *   attribute in the extension jar's manifest file. This class must implement {@link Authorizer} and have a default
 *   constructor.</li>
 *   <li>During {@link #get}, the instantiator creates an instance of the {@link Authorizer}
 *   class and also calls its {@link Authorizer#initialize(AuthorizationContext)} method with an
 *   {@link AuthorizationContext} created using a {@link AuthorizationContextFactory} by providing it a
 *   {@link Properties} object that is populated with all configuration settings from {@code cdap-site.xml} that have
 *   keys with the prefix {@link Constants.Security.Authorization#EXTENSION_CONFIG_PREFIX}.</li>
 *   <li>During {@link #close()}, the {@link Authorizer#destroy()} method is invoked, and the
 *   {@link AuthorizerClassLoader} is closed.</li>
 * </ul>
 */
public class AuthorizerInstantiator implements Closeable, Supplier<Authorizer> {

  private static final Logger LOG = LoggerFactory.getLogger(AuthorizerInstantiator.class);

  private final CConfiguration cConf;
  private final boolean authenticationEnabled;
  private final boolean authorizationEnabled;
  private final InstantiatorFactory instantiatorFactory;
  private final AuthorizationContextFactory authorizationContextFactory;

  private File tmpDir;
  private AuthorizerClassLoader authorizerClassLoader;
  private Authorizer authorizer;

  @Inject
  @VisibleForTesting
  public AuthorizerInstantiator(CConfiguration cConf, AuthorizationContextFactory authorizationContextFactory) {
    this.cConf = cConf;
    this.authenticationEnabled = cConf.getBoolean(Constants.Security.ENABLED);
    this.authorizationEnabled = cConf.getBoolean(Constants.Security.Authorization.ENABLED);
    this.instantiatorFactory = new InstantiatorFactory(false);
    this.authorizationContextFactory = authorizationContextFactory;
  }

  /**
   * Returns an instance of the configured {@link Authorizer} extension, or of {@link NoOpAuthorizer}, if
   * authorization is disabled.
   */
  @Override
  public synchronized Authorizer get() {
    if (authorizer != null) {
      return authorizer;
    }

    if (!authorizationEnabled) {
      LOG.debug("Authorization is disabled. Using a no-op authorizer.");
      authorizer = new NoOpAuthorizer();
      return authorizer;
    }
    if (!authenticationEnabled) {
      LOG.debug("Authorization is enabled. However, authentication is disabled. Using a no-op authorizer.");
      authorizer = new NoOpAuthorizer();
      return authorizer;
    }
    // Authorization is enabled, so continue with startup now
    String authorizerExtensionJarPath = cConf.get(Constants.Security.Authorization.EXTENSION_JAR_PATH);
    if (Strings.isNullOrEmpty(authorizerExtensionJarPath)) {
      throw new IllegalArgumentException(
        String.format("Authorizer extension jar path not found in configuration. Please set %s in cdap-site.xml to " +
                        "the fully qualified path of the jar file to use as the authorization backend.",
                      Constants.Security.Authorization.EXTENSION_JAR_PATH));
    }
    try {
      File authorizerExtensionJar = new File(authorizerExtensionJarPath);
      ensureValidAuthExtensionJar(authorizerExtensionJar);
      File absoluteTmpFile = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                      cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
      tmpDir = DirUtils.createTempDir(absoluteTmpFile);
      authorizerClassLoader = createAuthorizerClassLoader(authorizerExtensionJar);
      authorizer = createAndInitializeAuthorizerInstance(authorizerExtensionJar);
    } catch (Exception e) {
      Throwables.propagate(e);
    }
    return authorizer;
  }

  /**
   * Creates a new instance of the configured {@link Authorizer} extension, based on the provided extension jar
   * file.
   *
   * @return a new instance of the configured {@link Authorizer} extension
   */
  private Authorizer createAndInitializeAuthorizerInstance(File authorizerExtensionJar)
    throws IOException, InvalidAuthorizerException {
    Class<? extends Authorizer> authorizerClass = loadAuthorizerClass(authorizerExtensionJar);
    // Set the context class loader to the AuthorizerClassLoader before creating a new instance of the extension,
    // so all classes required in this process are created from the AuthorizerClassLoader.
    ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(authorizerClassLoader);
    LOG.debug("Setting context classloader to {}. Old classloader was {}.", authorizerClassLoader, oldClassLoader);
    try {
      Authorizer authorizer;
      try {
        authorizer = instantiatorFactory.get(TypeToken.of(authorizerClass)).create();
      } catch (Exception e) {
        throw new InvalidAuthorizerException(
          String.format("Error while instantiating for authorizer extension %s. Please make sure that the extension " +
                          "is a public class with a default constructor.", authorizerClass.getName()), e);
      }
      AuthorizationContext context = authorizationContextFactory.create(createExtensionProperties());
      try {
        authorizer.initialize(context);
      } catch (Exception e) {
        throw new InvalidAuthorizerException(
          String.format("Error while initializing authorizer extension %s.", authorizerClass.getName()), e);
      }
      return authorizer;
    } finally {
      // After the process of creation of a new instance has completed (success or failure), reset the context
      // classloader back to the original class loader.
      ClassLoaders.setContextClassLoader(oldClassLoader);
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

  @Override
  public void close() throws IOException {
    if (authorizer != null) {
      try {
        authorizer.destroy();
      } catch (Throwable t) {
        LOG.warn("Failed to destroy authorizer.", t);
      }
    }

    if (!authorizationEnabled || !authenticationEnabled) {
      // nothing to close, since we would not have created a class loader
      return;
    }

    if (authorizerClassLoader != null) {
      try {
        authorizerClassLoader.close();
      } catch (Throwable t) {
        LOG.warn("Failed to close authorizer class loader", t);
      }
    }

    if (tmpDir != null) {
      try {
        DirUtils.deleteDirectoryContents(tmpDir);
      } catch (Throwable t) {
        // It's a cleanup step. Nothing much can be done if cleanup fails.
        LOG.warn("Failed to delete directory {}", tmpDir, t);
      }
    }
  }
}
