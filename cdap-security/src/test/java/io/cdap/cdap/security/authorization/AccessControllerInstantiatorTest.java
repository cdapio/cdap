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

package io.cdap.cdap.security.authorization;

import com.google.common.base.Throwables;
import com.google.gson.Gson;
import io.cdap.cdap.common.FeatureDisabledException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.proto.security.GrantedPermission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Role;
import io.cdap.cdap.security.spi.authorization.AccessController;
import io.cdap.cdap.security.spi.authorization.AuthorizationContext;
import io.cdap.cdap.security.spi.authorization.NoOpAccessController;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 * Tests for {@link AccessControllerInstantiator}.
 */
public class AccessControllerInstantiatorTest extends AuthorizationTestBase {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testAuthenticationDisabled() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMPORARY_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
    assertDisabled(cConf, FeatureDisabledException.Feature.AUTHENTICATION);
  }

  @Test
  public void testAuthorizationDisabled() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMPORARY_FOLDER.newFolder().getAbsolutePath());
    assertDisabled(cConf, FeatureDisabledException.Feature.AUTHORIZATION);
  }

  private void assertDisabled(CConfiguration cConf, FeatureDisabledException.Feature feature) throws IOException {
    try (AccessControllerInstantiator instantiator = new AccessControllerInstantiator(cConf, AUTH_CONTEXT_FACTORY)) {
      AccessController accessController = instantiator.get();
      Assert.assertTrue(
        String.format("When %s is disabled, a %s must be returned, but got %s.",
                      feature.name().toLowerCase(), NoOpAccessController.class.getSimpleName(),
                      accessController.getClass().getName()),
        accessController instanceof NoOpAccessController
      );
    }
  }

  @Test(expected = InvalidAccessControllerException.class)
  public void testNonExistingAccessControllerJarPath() throws Throwable {
    CCONF.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, "/path/to/external-test-accessController.jar");
    try (AccessControllerInstantiator instantiator = new AccessControllerInstantiator(CCONF, AUTH_CONTEXT_FACTORY)) {
      instantiator.get();
      Assert.fail("Instantiation of AccessController should have failed because extension jar does not exist.");
    } catch (Throwable e) {
      throw Throwables.getRootCause(e);
    }
  }

  @Test(expected = InvalidAccessControllerException.class)
  public void testAccessControllerJarPathIsDirectory() throws Throwable {
    CCONF.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, TEMPORARY_FOLDER.newFolder().getPath());
    try (AccessControllerInstantiator instantiator = new AccessControllerInstantiator(CCONF, AUTH_CONTEXT_FACTORY)) {
      instantiator.get();
      Assert.fail("Instantiation of AccessController should have failed because extension jar is a directory");
    } catch (Throwable e) {
      throw Throwables.getRootCause(e);
    }
  }

  @Test(expected = InvalidAccessControllerException.class)
  public void testAccessControllerJarPathIsNotJar() throws Throwable {
    CCONF.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, TEMPORARY_FOLDER.newFile("abc.txt").getPath());
    try (AccessControllerInstantiator instantiator = new AccessControllerInstantiator(CCONF, AUTH_CONTEXT_FACTORY)) {
      instantiator.get();
      Assert.fail("Instantiation of AccessController should have failed because extension jar is not a jar file");
    } catch (Throwable e) {
      throw Throwables.getRootCause(e);
    }
  }

  @Test(expected = InvalidAccessControllerException.class)
  public void testMissingManifest() throws Throwable {
    Location externalAuthJar = createInvalidExternalAuthJar(null);
    CCONF.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, externalAuthJar.toString());
    try (AccessControllerInstantiator instantiator = new AccessControllerInstantiator(CCONF, AUTH_CONTEXT_FACTORY)) {
      instantiator.get();
      Assert.fail("Instantiation of AccessController should have failed " +
                    "because extension jar does not have a manifest");
    } catch (Throwable e) {
      throw Throwables.getRootCause(e);
    }
  }

  @Test(expected = InvalidAccessControllerException.class)
  public void testMissingAccessControllerClassName() throws Throwable {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    Location externalAuthJar = createInvalidExternalAuthJar(manifest);
    CCONF.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, externalAuthJar.toString());
    try (AccessControllerInstantiator instantiator = new AccessControllerInstantiator(CCONF, AUTH_CONTEXT_FACTORY)) {
      instantiator.get();
      Assert.fail("Instantiation of AccessController should have failed because extension jar's manifest does not " +
                    "define AccessController class.");
    } catch (Throwable e) {
      throw Throwables.getRootCause(e);
    }
  }

  @Test(expected = InvalidAccessControllerException.class)
  public void testDoesNotImplementAccessController() throws Throwable {
    Manifest manifest = new Manifest();
    Attributes mainAttributes = manifest.getMainAttributes();
    mainAttributes.put(Attributes.Name.MAIN_CLASS, DoesNotImplementAccessController.class.getName());
    Location externalAuthJar = AppJarHelper.createDeploymentJar(locationFactory, DoesNotImplementAccessController.class,
                                                                manifest);
    CCONF.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, externalAuthJar.toString());
    try (AccessControllerInstantiator instantiator = new AccessControllerInstantiator(CCONF, AUTH_CONTEXT_FACTORY)) {
      instantiator.get();
      Assert.fail("Instantiation of AccessController should have failed because the AccessController class defined " +
                    "in the extension jar's manifest does not implement " + AccessController.class.getName());
    } catch (Throwable e) {
      throw Throwables.getRootCause(e);
    }
  }

  @Test(expected = InvalidAccessControllerException.class)
  public void testInitializationThrowsException() throws Throwable {
    Manifest manifest = new Manifest();
    Attributes mainAttributes = manifest.getMainAttributes();
    mainAttributes.put(Attributes.Name.MAIN_CLASS, ExceptionInInitialize.class.getName());
    Location externalAuthJar = AppJarHelper.createDeploymentJar(locationFactory, ExceptionInInitialize.class,
                                                                manifest);
    CCONF.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, externalAuthJar.toString());
    try (AccessControllerInstantiator instantiator = new AccessControllerInstantiator(CCONF, AUTH_CONTEXT_FACTORY)) {
      instantiator.get();
      Assert.fail("Instantiation of AccessController should have failed because the AccessController class defined " +
                    "in the extension jar's manifest does not implement " + AccessController.class.getName());
    } catch (Throwable e) {
      throw e.getCause();
    }
  }

  @Test
  public void testAccessControllerExtension() throws Exception {
    Location externalAuthJar = createValidAuthExtensionJar();
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    cConfCopy.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, externalAuthJar.toString());

    // Create a temporary file.
    final File tempFile = TEMP_FOLDER.newFile("conf-file.xml");

    cConfCopy.set(Constants.Security.Authorization.EXTENSION_EXTRA_CLASSPATH, tempFile.getParent());

    try (AccessControllerInstantiator instantiator =
           new AccessControllerInstantiator(cConfCopy, AUTH_CONTEXT_FACTORY)) {
      // should be able to load the ExternalAccessController class via the AccessControllerInstantiatorService
      AccessController externalAccessController1 = instantiator.get();
      externalAccessController1.listAllRoles();
      externalAccessController1.listGrants(new Principal("test", Principal.PrincipalType.USER));

      ClassLoader accessControllerClassLoader = externalAccessController1.getClass().getClassLoader();

      // should be able to load the ExternalAccessController class via the AccessControllerClassLoader
      accessControllerClassLoader.loadClass(ValidExternalAccessController.class.getName());
      Assert.assertNotNull(accessControllerClassLoader.getResource("conf-file.xml"));
    }
  }

  @Test
  public void testAccessControllerExtensionExtraClasspath() throws IOException, ClassNotFoundException {
    Location externalAuthJar = createValidAuthExtensionJar();
    CConfiguration cConfCopy = CConfiguration.copy(CCONF);
    cConfCopy.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, externalAuthJar.toString());
    cConfCopy.set(Constants.Security.Authorization.EXTENSION_CONFIG_PREFIX + "config.path",
                  "/path/config.ini");
    cConfCopy.set(Constants.Security.Authorization.EXTENSION_CONFIG_PREFIX + "service.address",
                  "http://foo.bar.co:5555");
    cConfCopy.set(Constants.Security.Authorization.EXTENSION_CONFIG_PREFIX + "cache.ttl.secs",
                  "500");
    cConfCopy.set(Constants.Security.Authorization.EXTENSION_CONFIG_PREFIX + "cache.max.entries",
                  "50000");
    cConfCopy.set("foo." + Constants.Security.Authorization.EXTENSION_CONFIG_PREFIX + "dont.include",
                  "not.prefix.should.not.be.included");
    try (AccessControllerInstantiator instantiator =
           new AccessControllerInstantiator(cConfCopy, AUTH_CONTEXT_FACTORY)) {
      // should be able to load the ExternalAccessController class via the AccessControllerInstantiatorService
      AccessController externalAccessController1 = instantiator.get();
      Assert.assertNotNull(externalAccessController1);
      AccessController externalAccessController2 = instantiator.get();
      Assert.assertNotNull(externalAccessController2);
      // verify that get returns the same  instance each time it is called.
      Assert.assertEquals(externalAccessController1, externalAccessController2);

      ClassLoader accessControllerClassLoader = externalAccessController1.getClass().getClassLoader();
      ClassLoader parent = accessControllerClassLoader.getParent();
      // should be able to load the AccessController interface via the parent
      parent.loadClass(AccessController.class.getName());
      // should not be able to load the ExternalAccessController class via the parent class loader
      try {
        parent.loadClass(ValidExternalAccessController.class.getName());
        Assert.fail("Should not be able to load external accessController classes via the parent classloader of the " +
                      "AccessController class loader.");
      } catch (ClassNotFoundException expected) {
        // expected
      }
      // should be able to load the ExternalAccessController class via the AccessControllerClassLoader
      accessControllerClassLoader.loadClass(ValidExternalAccessController.class.getName());

      // have to do this because the external accessController instance is created in a new classloader, so casting will
      // not work.
      Gson gson = new Gson();
      ValidExternalAccessController validAccessController = gson.fromJson(gson.toJson(externalAccessController1),
                                                                    ValidExternalAccessController.class);
      Properties expectedProps = new Properties();
      expectedProps.put("config.path", "/path/config.ini");
      expectedProps.put("service.address", "http://foo.bar.co:5555");
      expectedProps.put("cache.ttl.secs", "500");
      expectedProps.put("cache.max.entries", "50000");
      Properties actualProps = validAccessController.getProperties();
      Assert.assertEquals(expectedProps, actualProps);
    }
  }

  private Location createInvalidExternalAuthJar(@Nullable Manifest manifest) throws IOException {
    String jarName = "external-accessController";
    Location externalAuthJar = locationFactory.create(jarName).getTempFile(".jar");
    try (
      OutputStream out = externalAuthJar.getOutputStream();
      JarOutputStream jarOutput = manifest == null ? new JarOutputStream(out) : new JarOutputStream(out, manifest)
    ) {
      JarEntry entry = new JarEntry("dummy.class");
      jarOutput.putNextEntry(entry);
      jarOutput.closeEntry();
    }
    return externalAuthJar;
  }

  private Location createValidAuthExtensionJar() throws IOException {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, ValidExternalAccessController.class.getName());
    return AppJarHelper.createDeploymentJar(locationFactory, ValidExternalAccessController.class,
                                            manifest);
  }

  public static final class ExceptionInInitialize extends NoOpAccessController {
    @Override
    public void initialize(AuthorizationContext context) {
      throw new IllegalStateException("Testing exception during initialize");
    }
  }

  public static class ValidExternalAccessControllerBase extends NoOpAccessController {

    @Override
    public Set<Role> listAllRoles() {
      Assert.assertEquals(getClass().getClassLoader(), Thread.currentThread().getContextClassLoader());
      return super.listAllRoles();
    }
  }

  public static final class ValidExternalAccessController extends ValidExternalAccessControllerBase {
    private Properties properties;
    @Override
    public void initialize(AuthorizationContext context) {
      this.properties = context.getExtensionProperties();
    }

    public Properties getProperties() {
      return properties;
    }

    @Override
    public Set<GrantedPermission> listGrants(Principal principal) {
      assertContextClassLoader();
      return super.listGrants(principal);
    }

    private static void assertContextClassLoader() {
      Assert.assertEquals(ValidExternalAccessController.class.getClassLoader(),
                          Thread.currentThread().getContextClassLoader());
    }

  }

  private static final class DoesNotImplementAccessController {
  }
}
