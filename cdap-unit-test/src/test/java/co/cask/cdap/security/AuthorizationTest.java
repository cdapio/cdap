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

import co.cask.cdap.AllProgramsApp;
import co.cask.cdap.WorkflowApp;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.twill.LocalLocationFactory;
import co.cask.cdap.gateway.handlers.InMemoryAuthorizer;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import com.google.common.collect.ImmutableSet;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
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
  public void testApps() throws Exception {
    try {
      deployApplication(NamespaceId.DEFAULT.toId(), AllProgramsApp.class);
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
    ApplicationManager appManager = deployApplication(AUTH_NAMESPACE.toId(), AllProgramsApp.class);
    // alice should get all privileges on the app after deployment succeeds
    ApplicationId app = AUTH_NAMESPACE.app(AllProgramsApp.NAME);
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(instance, Action.ADMIN),
        new Privilege(AUTH_NAMESPACE, Action.ALL),
        new Privilege(app, Action.ALL)
      ),
      authorizer.listPrivileges(ALICE)
    );
    // Bob should not have any privileges on Alice's app
    Assert.assertTrue("Bob should not have any privileges on alice's app", authorizer.listPrivileges(BOB).isEmpty());
    // This is necessary because in tests, artifacts have auto-generated versions when an app is deployed without f
    // irst creating an artifact
    String version = appManager.getInfo().getArtifact().getVersion();
    // update should succeed because alice has admin privileges on the app
    appManager.update(new AppRequest(new ArtifactSummary(AllProgramsApp.class.getSimpleName(), version)));
    // Update should fail for Bob
    SecurityRequestContext.setUserId(BOB.getName());
    try {
      appManager.update(new AppRequest(new ArtifactSummary(AllProgramsApp.class.getSimpleName(), version)));
      Assert.fail("App update should have failed because Alice does not have admin privileges on the app.");
    } catch (UnauthorizedException expected) {
      // expected
    }
    // grant READ and WRITE to Bob
    authorizer.grant(app, BOB, ImmutableSet.of(Action.READ, Action.WRITE));
    // delete should fail
    try {
      appManager.delete();
    } catch (UnauthorizedException expected) {
      // expected
    }
    // grant ADMIN to Bob. Now delete should succeed
    authorizer.grant(app, BOB, ImmutableSet.of(Action.ADMIN));
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(app, Action.READ),
        new Privilege(app, Action.WRITE),
        new Privilege(app, Action.ADMIN)
      ),
      authorizer.listPrivileges(BOB)
    );
    appManager.delete();
    // All privileges on the app should have been revoked
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(instance, Action.ADMIN),
        new Privilege(AUTH_NAMESPACE, Action.ALL)
      ),
      authorizer.listPrivileges(ALICE)
    );
    Assert.assertTrue("Bob should not have any privileges because all privileges on the app have been revoked " +
                        "since the app got deleted", authorizer.listPrivileges(BOB).isEmpty());
    // switch back to Alice
    SecurityRequestContext.setUserId(ALICE.getName());
    // Deploy a couple of apps in the namespace
    deployApplication(AUTH_NAMESPACE.toId(), AllProgramsApp.class);
    deployApplication(AUTH_NAMESPACE.toId(), WorkflowApp.class);
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(instance, Action.ADMIN),
        new Privilege(AUTH_NAMESPACE, Action.ALL),
        new Privilege(AUTH_NAMESPACE.app(AllProgramsApp.NAME), Action.ALL),
        new Privilege(AUTH_NAMESPACE.app(WorkflowApp.class.getSimpleName()), Action.ALL)
      ),
      authorizer.listPrivileges(ALICE)
    );
    // revoke all privileges on an app.
    authorizer.revoke(AUTH_NAMESPACE.app(WorkflowApp.class.getSimpleName()));
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(instance, Action.ADMIN),
        new Privilege(AUTH_NAMESPACE, Action.ALL),
        new Privilege(AUTH_NAMESPACE.app(AllProgramsApp.NAME), Action.ALL)
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
        new Privilege(AUTH_NAMESPACE.app(WorkflowApp.class.getSimpleName()), Action.ADMIN)
      ),
      authorizer.listPrivileges(ALICE)
    );
    deleteAllApplications(AUTH_NAMESPACE);
    // deleting all apps should remove all privileges on all apps, but the privilege on the namespace should still exist
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
    // we want to execute TestBase's @AfterClass after unsetting userid
    SecurityRequestContext.setUserId(OLD_USER);
    finish();
  }
}
