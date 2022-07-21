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

package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.inject.Injector;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.app.program.ManifestFields;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.runtime.artifact.app.plugin.PluginTestApp;
import io.cdap.cdap.internal.app.runtime.artifact.app.plugin.PluginTestRunnable;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.GrantedPermission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.authorization.AccessControllerInstantiator;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.AccessController;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.jar.Manifest;

/**
 * Tests for authorization for system artifacts. These tests are not in AuthorizationTest, because we do not want to
 * expose system artifacts capabilities in TestBase. This has to be in its own class because we want to enable
 * authorization in the CDAP that this test starts up.
 */
public class SystemArtifactsAuthorizationTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static final Principal ALICE = new Principal("alice", Principal.PrincipalType.USER);
  private static final String OLD_USER_ID = SecurityRequestContext.getUserId();
  private static final ArtifactId SYSTEM_ARTIFACT = NamespaceId.SYSTEM.artifact("system-artifact", "1.0.0");

  private static ArtifactRepository artifactRepository;
  private static AccessController accessController;
  private static NamespaceAdmin namespaceAdmin;

  @BeforeClass
  public static void setup() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setBoolean(Constants.Security.ENABLED, true);
    cConf.setBoolean(Constants.Security.KERBEROS_ENABLED, false);
    cConf.setBoolean(Constants.Security.Authorization.ENABLED, true);
    cConf.setInt(Constants.Security.Authorization.CACHE_MAX_ENTRIES, 0);
    Location deploymentJar = AppJarHelper.createDeploymentJar(new LocalLocationFactory(TMP_FOLDER.newFolder()),
                                                              InMemoryAccessController.class);
    cConf.set(Constants.Security.Authorization.EXTENSION_JAR_PATH, deploymentJar.toURI().getPath());

    // Add a system artifact
    File systemArtifactsDir = TMP_FOLDER.newFolder();
    cConf.set(Constants.AppFabric.SYSTEM_ARTIFACTS_DIR, systemArtifactsDir.getAbsolutePath());
    createSystemArtifact(systemArtifactsDir);
    Injector injector = AppFabricTestHelper.getInjector(cConf);
    artifactRepository = injector.getInstance(ArtifactRepository.class);
    AccessControllerInstantiator instantiatorService = injector.getInstance(AccessControllerInstantiator.class);
    accessController = instantiatorService.get();
    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
  }

  @Test
  public void testAuthorizationForSystemArtifacts() throws Exception {
    artifactRepository.addSystemArtifacts();
    // alice should not be able to refresh system artifacts because she does not have admin privileges on namespace
    // system
    SecurityRequestContext.setUserId(ALICE.getName());
    try {
      artifactRepository.addSystemArtifacts();
      Assert.fail("Adding system artifacts should have failed because alice does not have admin privileges on " +
                    "the namespace system.");
    } catch (UnauthorizedException expected) {
      // expected
    }
    // grant alice admin privileges on the CDAP system namespace
    Authorizable authorizable = Authorizable.fromEntityId(NamespaceId.SYSTEM, EntityType.ARTIFACT);
    accessController.grant(authorizable, ALICE,
                           Collections.singleton(StandardPermission.CREATE));
    Assert.assertEquals(
      Collections.singleton(new GrantedPermission(authorizable, StandardPermission.CREATE)),
      accessController.listGrants(ALICE));
    // refreshing system artifacts should succeed now
    artifactRepository.addSystemArtifacts();
    SecurityRequestContext.setUserId("bob");
    // deleting a system artifact should fail because bob does not have admin privileges on the artifact
    try {
      artifactRepository.deleteArtifact(Id.Artifact.fromEntityId(SYSTEM_ARTIFACT));
      Assert.fail("Deleting a system artifact should have failed because alice does not have admin privileges on " +
                    "the artifact.");
    } catch (UnauthorizedException expected) {
      // expected
    }

    // grant alice admin privileges on test namespace
    SecurityRequestContext.setUserId(ALICE.getName());
    NamespaceId namespaceId = new NamespaceId("test");
    accessController.grant(Authorizable.fromEntityId(namespaceId), ALICE,
                           EnumSet.allOf(StandardPermission.class));
    accessController.grant(Authorizable.fromEntityId(namespaceId, EntityType.ARTIFACT), ALICE,
                           EnumSet.of(StandardPermission.LIST));
    namespaceAdmin.create(new NamespaceMeta.Builder().setName(namespaceId.getNamespace()).build());

    // test that system artifacts are available to everyone
    List<ArtifactSummary> artifacts = artifactRepository.getArtifactSummaries(namespaceId, true);
    Assert.assertEquals(1, artifacts.size());
    ArtifactSummary artifactSummary = artifacts.get(0);
    Assert.assertEquals(SYSTEM_ARTIFACT.getArtifact(), artifactSummary.getName());
    Assert.assertEquals(SYSTEM_ARTIFACT.getVersion(), artifactSummary.getVersion());
    Assert.assertEquals(SYSTEM_ARTIFACT.getNamespace(), artifactSummary.getScope().name().toLowerCase());

    // test the getArtifact API
    ArtifactDetail artifactDetail = artifactRepository.getArtifact(Id.Artifact.fromEntityId(SYSTEM_ARTIFACT));
    io.cdap.cdap.api.artifact.ArtifactId artifactId = artifactDetail.getDescriptor().getArtifactId();
    Assert.assertEquals(SYSTEM_ARTIFACT.getArtifact(), artifactId.getName());
    Assert.assertEquals(SYSTEM_ARTIFACT.getVersion(), artifactId.getVersion().getVersion());
    Assert.assertEquals(SYSTEM_ARTIFACT.getNamespace(), artifactId.getScope().name().toLowerCase());

    namespaceAdmin.delete(namespaceId);
    // enforce on the system artifact should fail in unit test, since we do not have auto-grant now
    try {
      accessController.enforce(SYSTEM_ARTIFACT, ALICE, EnumSet.allOf(StandardPermission.class));
      Assert.fail();
    } catch (UnauthorizedException e) {
      // expected
    }

    try {
      artifactRepository.deleteArtifact(Id.Artifact.fromEntityId(SYSTEM_ARTIFACT));
      Assert.fail();
    } catch (UnauthorizedException e) {
      // expected
    }
    // deleting system artifact should succeed if alice has DELETE on the artifact
    accessController.grant(Authorizable.fromEntityId(SYSTEM_ARTIFACT), ALICE, EnumSet.of(StandardPermission.DELETE));
    artifactRepository.deleteArtifact(Id.Artifact.fromEntityId(SYSTEM_ARTIFACT));

    // clean up privilege
    accessController.revoke(Authorizable.fromEntityId(SYSTEM_ARTIFACT));
    accessController.revoke(Authorizable.fromEntityId(NamespaceId.SYSTEM, EntityType.ARTIFACT));
    accessController.revoke(Authorizable.fromEntityId(namespaceId, EntityType.ARTIFACT));
    accessController.revoke(Authorizable.fromEntityId(namespaceId));
  }

  @AfterClass
  public static void cleanup() throws Exception {
    accessController.revoke(Authorizable.fromEntityId(NamespaceId.SYSTEM));
    Assert.assertEquals(Collections.emptySet(), accessController.listGrants(ALICE));
    SecurityRequestContext.setUserId(OLD_USER_ID);
    AppFabricTestHelper.shutdown();
  }

  private static void createSystemArtifact(File systemArtifactsDir) throws IOException {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, PluginTestRunnable.class.getPackage().getName());
    File systemArtifact = new File(
      systemArtifactsDir, String.format("%s-%s.jar", SYSTEM_ARTIFACT.getArtifact(), SYSTEM_ARTIFACT.getVersion())
    );
    createAppJar(PluginTestApp.class, systemArtifact, manifest);
  }

  private static File createAppJar(Class<?> cls, File destFile, Manifest manifest) throws IOException {
    Location deploymentJar = AppJarHelper.createDeploymentJar(new LocalLocationFactory(TMP_FOLDER.newFolder()),
                                                              cls, manifest);
    DirUtils.mkdirs(destFile.getParentFile());
    Locations.linkOrCopyOverwrite(deploymentJar, destFile);
    return destFile;
  }
}
