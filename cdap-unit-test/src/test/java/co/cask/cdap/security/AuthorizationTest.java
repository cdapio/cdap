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

package co.cask.cdap.security;

import co.cask.cdap.ConfigTestApp;
import co.cask.cdap.WorkflowApp;
import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.twill.LocalLocationFactory;
import co.cask.cdap.gateway.handlers.InMemoryAuthorizer;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedArtifactId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ArtifactManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.XSlowTests;
import co.cask.cdap.test.app.DummyApp;
import co.cask.cdap.test.artifacts.plugins.ToStringPlugin;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.io.IOException;

/**
 * Unit tests with authorization enabled.
 */
public class AuthorizationTest extends TestBase {

  private static final String OLD_USER = SecurityRequestContext.getUserId();
  private static final Principal ALICE = new Principal("alice", Principal.PrincipalType.USER);
  private static final Principal BOB = new Principal("bob", Principal.PrincipalType.USER);
  private static final NamespaceId AUTH_NAMESPACE = new NamespaceId("authorization");
  private static final NamespaceMeta AUTH_NAMESPACE_META =
    new NamespaceMeta.Builder().setName(AUTH_NAMESPACE.getNamespace()).build();

  private static InstanceId instance;

  /**
   * An {@link ExternalResource} that wraps a {@link TemporaryFolder} and {@link TestConfiguration} to execute them in
   * a chain.
   */
  private static final class AuthTestConf extends ExternalResource {
    private final TemporaryFolder tmpFolder = new TemporaryFolder();
    private TestConfiguration testConf;

    @Override
    public Statement apply(final Statement base, final Description description) {
      // Apply the TemporaryFolder on a Statement that creates a TestConfiguration and applies on base
      return tmpFolder.apply(new Statement() {
        @Override
        public void evaluate() throws Throwable {
          testConf = new TestConfiguration(getAuthConfigs(tmpFolder.newFolder()));
          testConf.apply(base, description).evaluate();
        }
      }, description);
    }

    private static String[] getAuthConfigs(File tmpDir) throws IOException {
      LocationFactory locationFactory = new LocalLocationFactory(tmpDir);
      Location authExtensionJar = AppJarHelper.createDeploymentJar(locationFactory, InMemoryAuthorizer.class);
      return new String[] {
        Constants.Security.Authorization.ENABLED, "true",
        Constants.Security.Authorization.EXTENSION_JAR_PATH, authExtensionJar.toURI().getPath()
      };
    }
  }

  @ClassRule
  public static final AuthTestConf AUTH_TEST_CONF = new AuthTestConf();

  @BeforeClass
  public static void setup() {
    instance = new InstanceId(getConfiguration().get(Constants.INSTANCE_NAME));
    SecurityRequestContext.setUserId(ALICE.getName());
  }

  @Before
  public void setupTest() throws Exception {
    Assert.assertEquals(ImmutableSet.of(), getAuthorizer().listPrivileges(ALICE));
  }

  @Test
  public void testNamespaces() throws Exception {
    NamespaceAdmin namespaceAdmin = getNamespaceAdmin();
    Authorizer authorizer = getAuthorizer();
    try {
      namespaceAdmin.create(AUTH_NAMESPACE_META);
      Assert.fail("Namespace create should have failed because alice is not authorized on " + instance);
    } catch (UnauthorizedException expected) {
      // expected
    }
    authorizer.grant(instance, ALICE, ImmutableSet.of(Action.ADMIN));
    Assert.assertEquals(ImmutableSet.of(new Privilege(instance, Action.ADMIN)), authorizer.listPrivileges(ALICE));
    namespaceAdmin.create(AUTH_NAMESPACE_META);
    // create should grant all privileges
    Assert.assertEquals(
      ImmutableSet.of(new Privilege(instance, Action.ADMIN), new Privilege(AUTH_NAMESPACE, Action.ALL)),
      authorizer.listPrivileges(ALICE)
    );
    // No authorization currently for listing and retrieving namespace
    namespaceAdmin.list();
    namespaceAdmin.get(AUTH_NAMESPACE.toId());
    // revoke privileges
    authorizer.revoke(AUTH_NAMESPACE);
    Assert.assertEquals(ImmutableSet.of(new Privilege(instance, Action.ADMIN)), authorizer.listPrivileges(ALICE));
    try {
      namespaceAdmin.deleteDatasets(AUTH_NAMESPACE.toId());
      Assert.fail("Namespace delete datasets should have failed because alice's privileges on the namespace have " +
                    "been revoked");
    } catch (UnauthorizedException expected) {
      // expected
    }
    // grant privileges again
    authorizer.grant(AUTH_NAMESPACE, ALICE, ImmutableSet.of(Action.ADMIN));
    Assert.assertEquals(
      ImmutableSet.of(new Privilege(instance, Action.ADMIN), new Privilege(AUTH_NAMESPACE, Action.ADMIN)),
      authorizer.listPrivileges(ALICE)
    );
    namespaceAdmin.deleteDatasets(AUTH_NAMESPACE.toId());
    // deleting datasets does not revoke privileges.
    Assert.assertEquals(
      ImmutableSet.of(new Privilege(instance, Action.ADMIN), new Privilege(AUTH_NAMESPACE, Action.ADMIN)),
      authorizer.listPrivileges(ALICE)
    );
    NamespaceMeta updated = new NamespaceMeta.Builder(AUTH_NAMESPACE_META).setDescription("new desc").build();
    namespaceAdmin.updateProperties(AUTH_NAMESPACE.toId(), updated);
    namespaceAdmin.delete(AUTH_NAMESPACE.toId());
    // once the namespace has been deleted, privileges on that namespace should be revoked
    Assert.assertEquals(ImmutableSet.of(new Privilege(instance, Action.ADMIN)), authorizer.listPrivileges(ALICE));
    // revoke alice's ADMIN privilege on the instance, so we get back to the same state as at the beginning of the test
    authorizer.revoke(instance);
    Assert.assertEquals(ImmutableSet.of(), authorizer.listPrivileges(ALICE));
  }

  @Test
  @Category(XSlowTests.class)
  public void testApps() throws Exception {
    try {
      deployApplication(NamespaceId.DEFAULT.toId(), DummyApp.class);
      Assert.fail("App deployment should fail because alice does not have WRITE access on the default namespace");
    } catch (IllegalStateException expected) {
      // expected
    }
    Authorizer authorizer = getAuthorizer();
    authorizer.grant(instance, ALICE, ImmutableSet.of(Action.ADMIN));
    getNamespaceAdmin().create(AUTH_NAMESPACE_META);
    Assert.assertEquals(
      ImmutableSet.of(new Privilege(instance, Action.ADMIN), new Privilege(AUTH_NAMESPACE, Action.ALL)),
      authorizer.listPrivileges(ALICE)
    );
    // deployment should succeed in the authorized namespace because alice has all privileges on it
    ApplicationManager appManager = deployApplication(AUTH_NAMESPACE.toId(), DummyApp.class);
    // alice should get all privileges on the app after deployment succeeds
    ApplicationId dummyAppId = AUTH_NAMESPACE.app(DummyApp.class.getSimpleName());
    ArtifactSummary artifact = appManager.getInfo().getArtifact();
    NamespacedArtifactId dummyArtifact =
      Ids.namespace(dummyAppId.getNamespace()).artifact(artifact.getName(), artifact.getVersion());
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(instance, Action.ADMIN),
        new Privilege(AUTH_NAMESPACE, Action.ALL),
        new Privilege(dummyAppId, Action.ALL),
        new Privilege(dummyArtifact, Action.ALL)
      ),
      authorizer.listPrivileges(ALICE)
    );
    // Bob should not have any privileges on Alice's app
    Assert.assertTrue("Bob should not have any privileges on alice's app", authorizer.listPrivileges(BOB).isEmpty());
    // This is necessary because in tests, artifacts have auto-generated versions when an app is deployed without
    // first creating an artifact
    String version = artifact.getVersion();
    // update should succeed because alice has admin privileges on the app
    appManager.update(new AppRequest(artifact));
    // Update should fail for Bob
    SecurityRequestContext.setUserId(BOB.getName());
    try {
      appManager.update(new AppRequest(new ArtifactSummary(DummyApp.class.getSimpleName(), version)));
      Assert.fail("App update should have failed because Alice does not have admin privileges on the app.");
    } catch (UnauthorizedException expected) {
      // expected
    }
    // grant READ and WRITE to Bob
    authorizer.grant(dummyAppId, BOB, ImmutableSet.of(Action.READ, Action.WRITE));
    // delete should fail
    try {
      appManager.delete();
    } catch (UnauthorizedException expected) {
      // expected
    }
    // grant ADMIN to Bob. Now delete should succeed
    authorizer.grant(dummyAppId, BOB, ImmutableSet.of(Action.ADMIN));
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(dummyAppId, Action.READ),
        new Privilege(dummyAppId, Action.WRITE),
        new Privilege(dummyAppId, Action.ADMIN)
      ),
      authorizer.listPrivileges(BOB)
    );
    appManager.delete();
    // All privileges on the app should have been revoked
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(instance, Action.ADMIN),
        new Privilege(AUTH_NAMESPACE, Action.ALL),
        new Privilege(dummyArtifact, Action.ALL)
      ),
      authorizer.listPrivileges(ALICE)
    );
    Assert.assertTrue("Bob should not have any privileges because all privileges on the app have been revoked " +
                        "since the app got deleted", authorizer.listPrivileges(BOB).isEmpty());
    // switch back to Alice
    SecurityRequestContext.setUserId(ALICE.getName());
    // Deploy a couple of apps in the namespace
    appManager = deployApplication(AUTH_NAMESPACE.toId(), DummyApp.class);
    artifact = appManager.getInfo().getArtifact();
    NamespacedArtifactId updatedDummyArtifact = AUTH_NAMESPACE.artifact(artifact.getName(), artifact.getVersion());
    appManager = deployApplication(AUTH_NAMESPACE.toId(), WorkflowApp.class);
    artifact = appManager.getInfo().getArtifact();
    NamespacedArtifactId workflowArtifact = AUTH_NAMESPACE.artifact(artifact.getName(), artifact.getVersion());
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(instance, Action.ADMIN),
        new Privilege(AUTH_NAMESPACE, Action.ALL),
        new Privilege(AUTH_NAMESPACE.app(DummyApp.class.getSimpleName()), Action.ALL),
        new Privilege(AUTH_NAMESPACE.app(WorkflowApp.class.getSimpleName()), Action.ALL),
        new Privilege(dummyArtifact, Action.ALL),
        new Privilege(updatedDummyArtifact, Action.ALL),
        new Privilege(workflowArtifact, Action.ALL)
      ),
      authorizer.listPrivileges(ALICE)
    );
    // revoke all privileges on an app.
    authorizer.revoke(AUTH_NAMESPACE.app(WorkflowApp.class.getSimpleName()));
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(instance, Action.ADMIN),
        new Privilege(AUTH_NAMESPACE, Action.ALL),
        new Privilege(AUTH_NAMESPACE.app(DummyApp.class.getSimpleName()), Action.ALL),
        new Privilege(dummyArtifact, Action.ALL),
        new Privilege(updatedDummyArtifact, Action.ALL),
        new Privilege(workflowArtifact, Action.ALL)
      ),
      authorizer.listPrivileges(ALICE)
    );
    // deleting all apps should fail because alice does not have admin privileges on the Workflow app
    try {
      deleteAllApplications(AUTH_NAMESPACE);
      Assert.fail("Deleting all applications in the namespace should have failed because alice does not have ADMIN " +
                    "privilege on the workflow app.");
    } catch (UnauthorizedException expected) {
      // expected
    }
    // grant admin privilege on the WorkflowApp. deleting all applications should succeed.
    authorizer.grant(AUTH_NAMESPACE.app(WorkflowApp.class.getSimpleName()), ALICE, ImmutableSet.of(Action.ADMIN));
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(instance, Action.ADMIN),
        new Privilege(AUTH_NAMESPACE, Action.ALL),
        new Privilege(AUTH_NAMESPACE.app(WorkflowApp.class.getSimpleName()), Action.ADMIN),
        new Privilege(dummyArtifact, Action.ALL),
        new Privilege(updatedDummyArtifact, Action.ALL),
        new Privilege(workflowArtifact, Action.ALL)
      ),
      authorizer.listPrivileges(ALICE)
    );
    deleteAllApplications(AUTH_NAMESPACE);
    // deleting all apps should remove all privileges on all apps, but the privilege on the namespace should still exist
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(instance, Action.ADMIN),
        new Privilege(AUTH_NAMESPACE, Action.ALL),
        new Privilege(dummyArtifact, Action.ALL),
        new Privilege(updatedDummyArtifact, Action.ALL),
        new Privilege(workflowArtifact, Action.ALL)
      ),
      authorizer.listPrivileges(ALICE)
    );
    // clean up. remove the namespace. all privileges on the namespace should be revoked
    getNamespaceAdmin().delete(AUTH_NAMESPACE.toId());
    Assert.assertEquals(ImmutableSet.of(new Privilege(instance, Action.ADMIN)), authorizer.listPrivileges(ALICE));
    // revoke privileges on the instance
    authorizer.revoke(instance);
    Assert.assertEquals(ImmutableSet.of(), authorizer.listPrivileges(ALICE));
  }

  @Test
  public void testArtifacts() throws Exception {
    ArtifactId appArtifact = new ArtifactId("app-artifact", new ArtifactVersion("1.1.1"), ArtifactScope.USER);
    try {
      NamespacedArtifactId defaultNsArtifact = new NamespacedArtifactId(
        NamespaceId.DEFAULT.getNamespace(), appArtifact.getName(), appArtifact.getVersion().getVersion());
      addAppArtifact(defaultNsArtifact, ConfigTestApp.class);
      Assert.fail("Should not be able to add an app artifact to the default namespace because alice does not have " +
                    "write privileges on the default namespace.");
    } catch (UnauthorizedException expected) {
      // expected
    }
    ArtifactId pluginArtifact = new ArtifactId("plugin-artifact", new ArtifactVersion("1.2.3"), ArtifactScope.USER);
    try {
      NamespacedArtifactId defaultNsArtifact = new NamespacedArtifactId(
        NamespaceId.DEFAULT.getNamespace(), pluginArtifact.getName(), pluginArtifact.getVersion().getVersion());
      addAppArtifact(defaultNsArtifact, ToStringPlugin.class);
      Assert.fail("Should not be able to add a plugin artifact to the default namespace because alice does not have " +
                    "write privileges on the default namespace.");
    } catch (UnauthorizedException expected) {
      // expected
    }
    // create a new namespace, alice should get ALL privileges on the namespace
    Authorizer authorizer = getAuthorizer();
    authorizer.grant(instance, ALICE, ImmutableSet.of(Action.ADMIN));
    getNamespaceAdmin().create(AUTH_NAMESPACE_META);
    Assert.assertEquals(
      ImmutableSet.of(new Privilege(instance, Action.ADMIN), new Privilege(AUTH_NAMESPACE, Action.ALL)),
      authorizer.listPrivileges(ALICE)
    );
    // artifact deployment in this namespace should now succeed, and alice should have ALL privileges on the artifacts
    NamespacedArtifactId appArtifactId = new NamespacedArtifactId(
      AUTH_NAMESPACE.getNamespace(), appArtifact.getName(), appArtifact.getVersion().getVersion());
    ArtifactManager appArtifactManager = addAppArtifact(appArtifactId, ConfigTestApp.class);
    NamespacedArtifactId pluginArtifactId = new NamespacedArtifactId(
      AUTH_NAMESPACE.getNamespace(), pluginArtifact.getName(), pluginArtifact.getVersion().getVersion());
    ArtifactManager pluginArtifactManager = addPluginArtifact(pluginArtifactId, appArtifactId, ToStringPlugin.class);
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(instance, Action.ADMIN),
        new Privilege(AUTH_NAMESPACE, Action.ALL),
        new Privilege(appArtifactId, Action.ALL),
        new Privilege(pluginArtifactId, Action.ALL)
      ),
      authorizer.listPrivileges(ALICE)
    );
    // Bob should not be able to delete artifacts that he does not have ADMIN permission on
    SecurityRequestContext.setUserId(BOB.getName());
    try {
      appArtifactManager.writeProperties(ImmutableMap.of("authorized", "no"));
      Assert.fail("Writing properties to artifact should have failed because Bob does not have admin privileges on " +
                    "the artifact");
    } catch (UnauthorizedException expected) {
      // expected
    }

    try {
      appArtifactManager.delete();
      Assert.fail("Deleting artifact should have failed because Bob does not have admin privileges on the artifact");
    } catch (UnauthorizedException expected) {
      // expected
    }

    try {
      pluginArtifactManager.writeProperties(ImmutableMap.of("authorized", "no"));
      Assert.fail("Writing properties to artifact should have failed because Bob does not have admin privileges on " +
                    "the artifact");
    } catch (UnauthorizedException expected) {
      // expected
    }

    try {
      pluginArtifactManager.removeProperties();
      Assert.fail("Removing properties to artifact should have failed because Bob does not have admin privileges on " +
                    "the artifact");
    } catch (UnauthorizedException expected) {
      // expected
    }

    try {
      pluginArtifactManager.delete();
      Assert.fail("Deleting artifact should have failed because Bob does not have admin privileges on the artifact");
    } catch (UnauthorizedException expected) {
      // expected
    }
    // alice should be permitted to update properties/delete artifact
    SecurityRequestContext.setUserId(ALICE.getName());
    appArtifactManager.writeProperties(ImmutableMap.of("authorized", "yes"));
    appArtifactManager.removeProperties();
    appArtifactManager.delete();
    pluginArtifactManager.delete();
    // upon successful deletion, alice should lose all privileges on the artifact
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(instance, Action.ADMIN),
        new Privilege(AUTH_NAMESPACE, Action.ALL)
      ),
      authorizer.listPrivileges(ALICE)
    );
    // clean up. remove the namespace. all privileges on the namespace should be revoked
    getNamespaceAdmin().delete(AUTH_NAMESPACE.toId());
    Assert.assertEquals(ImmutableSet.of(new Privilege(instance, Action.ADMIN)), authorizer.listPrivileges(ALICE));
    // revoke privileges on the instance
    authorizer.revoke(instance);
    Assert.assertEquals(ImmutableSet.of(), authorizer.listPrivileges(ALICE));
  }

  @AfterClass
  public static void cleanup() throws Exception {
    // we want to execute TestBase's @AfterClass after unsetting userid, because the old userid has been granted ADMIN
    // on default namespace in TestBase so it can clean the namespace.
    SecurityRequestContext.setUserId(OLD_USER);
    finish();
  }
}
