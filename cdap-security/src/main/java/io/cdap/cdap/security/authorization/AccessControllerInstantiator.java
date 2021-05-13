/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package io.cdap.cdap.security.authorization;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.io.Closeables;
import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.lang.InstantiatorFactory;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.security.spi.authorization.AccessController;
import io.cdap.cdap.security.spi.authorization.AuthorizationContext;
import io.cdap.cdap.security.spi.authorization.Authorizer;
import io.cdap.cdap.security.spi.authorization.NoOpAccessController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.jar.Attributes;
import javax.annotation.Nullable;

/**
 * Class to instantiate {@link AccessController} extensions. Authorization extensions are instantiated using a
 * separate {@link ClassLoader} that is built using a bundled jar for the {@link AccessController} extension that
 * contains all its required dependencies. The {@link ClassLoader} is created with the parent as the classloader with
 * which the {@link AccessController} interface is instantiated. This parent only has classes required by the
 * {@code cdap-security-spi} module.
 *
 * The {@link AccessControllerInstantiator} has the following expectations from the extension:
 * <ul>
 *   <li>Authorization is enabled setting the parameter {@link Constants.Security.Authorization#ENABLED} to true in
 *   {@code cdap-site.xml}. When authorization is disabled, an instance of {@link NoOpAccessController} is returned.
 *   </li>
 *   <li>The path to the extension jar bundled with all its dependencies is read from the setting
 *   {@link Constants.Security.Authorization#EXTENSION_JAR_PATH} in cdap-site.xml</li>
 *   <li>The instantiator reads a fully qualified class name specified as the {@link Attributes.Name#MAIN_CLASS}
 *   attribute in the extension jar's manifest file. This class must implement {@link AccessController} and have
 *   a default constructor. If the extension depends on external jars or configuration files it is possible to provide
 *   them through {@link Constants.Security.Authorization#EXTENSION_EXTRA_CLASSPATH}</li>
 *   <li>During {@link #get}, the instantiator creates an instance of the {@link AccessController}
 *   class and also calls its {@link AccessController#initialize(AuthorizationContext)} method with an
 *   {@link AuthorizationContext} created using a {@link AuthorizationContextFactory} by providing it a
 *   {@link Properties} object that is populated with all configuration settings from {@code cdap-site.xml} that have
 *   keys with the prefix {@link Constants.Security.Authorization#EXTENSION_CONFIG_PREFIX}.</li>
 *   <li>During {@link #close()}, the {@link AccessController#destroy()} method is invoked, and the
 *   {@link AccessControllerClassLoader} is closed.</li>
 * </ul>
 */
public class AccessControllerInstantiator implements Closeable, Supplier<AccessController> {

  private static final Logger LOG = LoggerFactory.getLogger(AccessControllerInstantiator.class);
  private static final AccessController NOOP_ACCESS_CONTROLLER = new NoOpAccessController();

  private final CConfiguration cConf;
  private final InstantiatorFactory instantiatorFactory;
  private final AuthorizationContextFactory authorizationContextFactory;

  private volatile AccessController accessController;
  private AccessControllerClassLoader accessControllerClassLoader;
  private boolean closed;

  @Inject
  @VisibleForTesting
  public AccessControllerInstantiator(CConfiguration cConf, AuthorizationContextFactory authorizationContextFactory) {
    this.cConf = cConf;
    this.instantiatorFactory = new InstantiatorFactory(false);
    this.authorizationContextFactory = authorizationContextFactory;
  }

  /**
   * Returns an instance of the configured {@link AccessController} extension, or of {@link NoOpAccessController}, if
   * authorization is disabled.
   */
  @Override
  public AccessController get() {
    if (!cConf.getBoolean(Constants.Security.Authorization.ENABLED)) {
      LOG.debug("Authorization is disabled. Authorization can be enabled  by setting " +
                  Constants.Security.Authorization.ENABLED + " to true.");
      return NOOP_ACCESS_CONTROLLER;
    }
    if (!cConf.getBoolean(Constants.Security.ENABLED)) {
      LOG.warn("Authorization is enabled. However, authentication is disabled. Authorization policies will not be " +
                 "enforced. To enforce authorization policies please enable both authorization, by setting " +
                 Constants.Security.Authorization.ENABLED + " to true and authentication, by setting " +
                 Constants.Security.ENABLED + "to true.");
      return NOOP_ACCESS_CONTROLLER;
    }

    // Authorization is enabled
    AccessController accessController = this.accessController;
    if (accessController != null) {
      return accessController;
    }

    synchronized (this) {
      accessController = this.accessController;
      if (accessController != null) {
        return accessController;
      }

      if (closed) {
        throw new RuntimeException("Cannot create AccessController due to resources were closed");
      }

      String accessControllerExtensionJarPath = cConf.get(Constants.Security.Authorization.EXTENSION_JAR_PATH);
      String accessControllerExtraClasspath = cConf.get(Constants.Security.Authorization.EXTENSION_EXTRA_CLASSPATH);
      if (Strings.isNullOrEmpty(accessControllerExtensionJarPath)) {
        throw new IllegalArgumentException(
          String.format("Access control extension jar path not found in configuration. Please set %s in " +
                          "cdap-site.xml to the fully qualified path of the jar file to use as the authorization " +
                          "backend.",
                        Constants.Security.Authorization.EXTENSION_JAR_PATH));
      }
      try {
        File accessControllerExtensionJar = new File(accessControllerExtensionJarPath);
        ensureValidAuthExtensionJar(accessControllerExtensionJar);
        accessControllerClassLoader = createAccessControllerClassLoader(accessControllerExtensionJar,
                                                                        accessControllerExtraClasspath);
        this.accessController = accessController = createAccessController(accessControllerClassLoader);
        return accessController;
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Creates a new instance of the configured {@link AccessController} extension, based on the provided extension jar
   * file and initialize it.
   *
   * @return a new instance of the configured {@link AccessController} extension
   */
  private AccessController createAccessController(AccessControllerClassLoader classLoader)
    throws InvalidAccessControllerException {
    Class<?> accessControllerClass = loadAccessControllerClass(classLoader);
    // Set the context class loader to the AccessControllerClassLoader before creating a new instance of the extension,
    // so all classes required in this process are created from the AccessControllerClassLoader.
    ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(classLoader);
    LOG.trace("Setting context classloader to {}. Old classloader was {}.", classLoader, oldClassLoader);
    try {
      AccessController accessController;
      try {
        Object extensionClass = instantiatorFactory.get(TypeToken.of(accessControllerClass)).create();
        if (extensionClass instanceof AccessController) {
          accessController = (AccessController) extensionClass;
        } else {
          accessController = new AuthorizerWrapper((Authorizer) extensionClass);
        }
      } catch (Exception e) {
        throw new InvalidAccessControllerException(
          String.format("Error while instantiating for access controller extension %s. " +
                          "Please make sure that the extension " +
                          "is a public class with a default constructor.", accessControllerClass.getName()), e);
      }
      AuthorizationContext context = authorizationContextFactory.create(createExtensionProperties());
      try {
        accessController.initialize(context);
      } catch (Exception e) {
        throw new InvalidAccessControllerException(
          String.format("Error while initializing access control extension %s.", accessControllerClass.getName()), e);
      }
      return accessController;
    } finally {
      // After the process of creation of a new instance has completed (success or failure), reset the context
      // classloader back to the original class loader.
      ClassLoaders.setContextClassLoader(oldClassLoader);
      LOG.trace("Resetting context classloader to {} from {}.", oldClassLoader, classLoader);
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

  private AccessControllerClassLoader createAccessControllerClassLoader(File extensionJar,
                                                                        @Nullable String extraClasspath)
    throws InvalidAccessControllerException {
    LOG.info("Creating access control extension using jar {}.", extensionJar);

    File tmpDir = DirUtils.createTempDir(new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                                  cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile());
    try {
      return new AccessControllerClassLoader(tmpDir, extensionJar, extraClasspath);
    } catch (IOException e) {
      try {
        DirUtils.deleteDirectoryContents(tmpDir);
      } catch (IOException ex) {
        e.addSuppressed(ex);
      }
      throw new InvalidAccessControllerException("Failed to load access controle extension from "
                                                   + extensionJar + ".", e);
    }
  }

  @SuppressWarnings("unchecked")
  private Class<?> loadAccessControllerClass(AccessControllerClassLoader classLoader)
    throws InvalidAccessControllerException {

    String accessControllerClassName = classLoader.getAccessControllerClassName();
    Class<?> accessControllerClass;
    try {
      accessControllerClass = classLoader.loadClass(accessControllerClassName);
    } catch (ClassNotFoundException e) {
      throw new InvalidAccessControllerException(
        String.format("Access controle extension class %s not found. Please make sure that the right class " +
                        "is specified  in the extension jar's manifest located at %s.",
                      accessControllerClassName, classLoader.getExtensionJar()), e);
    }
    if (!Authorizer.class.isAssignableFrom(accessControllerClass)
      && !AccessController.class.isAssignableFrom(accessControllerClass)) {
      throw new InvalidAccessControllerException(
        String.format("Class %s defined as %s in the authorization extension's manifest at %s must implement %s or %s",
                      accessControllerClass.getName(), Attributes.Name.MAIN_CLASS, classLoader.getExtensionJar(),
                      AccessController.class.getName(), Authorizer.class.getName()));
    }
    return accessControllerClass;
  }

  private void ensureValidAuthExtensionJar(File accessControllerExtensionJar) throws InvalidAccessControllerException {
    if (!accessControllerExtensionJar.exists()) {
      throw new InvalidAccessControllerException(
        String.format("Authorization extension jar %s specified as %s does not exist.", accessControllerExtensionJar,
                      Constants.Security.Authorization.EXTENSION_JAR_PATH)
      );
    }
    if (!accessControllerExtensionJar.isFile()) {
      throw new InvalidAccessControllerException(
        String.format("Authorization extension jar %s specified as %s must be a file.", accessControllerExtensionJar,
                      Constants.Security.Authorization.EXTENSION_JAR_PATH)
      );
    }
  }

  @Override
  public void close() throws IOException {
    try {
      synchronized (this) {
        closed = true;
        AccessController accessController = this.accessController;

        if (accessController != null) {
          accessController.destroy();
        }
      }
    } catch (Throwable t) {
      LOG.warn("Failed to destroy accessController.", t);
    } finally {
      Closeables.closeQuietly(accessControllerClassLoader);
    }
  }
}
