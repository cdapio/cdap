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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.twill.LocalLocationFactory;
import co.cask.cdap.gateway.handlers.InMemoryAuthorizer;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Privilege;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
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

  private static final String oldUser = SecurityRequestContext.getUserId();
  private static final Principal alice = new Principal("alice", Principal.PrincipalType.USER);

  private InstanceId instance;

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
    SecurityRequestContext.setUserId(alice.getName());
  }

  @Before
  public void setupTest() throws Exception {
    instance = new InstanceId(getConfiguration().get(Constants.INSTANCE_NAME));
    getAuthorizer().grant(NamespaceId.DEFAULT, alice, ImmutableSet.of(Action.ADMIN));
    Assert.assertEquals(
      ImmutableSet.of(new Privilege(NamespaceId.DEFAULT, Action.ADMIN)),
      getAuthorizer().listPrivileges(alice)
    );
  }

  @Test
  public void testNamespaces() throws Exception {
    NamespaceId namespace = new NamespaceId("authorization");
    NamespaceAdmin namespaceAdmin = getNamespaceAdmin();
    Authorizer authorizer = getAuthorizer();
    NamespaceMeta meta = new NamespaceMeta.Builder().setName(namespace.getNamespace()).build();
    try {
      namespaceAdmin.create(meta);
      Assert.fail("Namespace create should have failed because alice is not authorized on " + instance);
    } catch (UnauthorizedException expected) {
      // expected
    }
    authorizer.grant(instance, alice, ImmutableSet.of(Action.ADMIN));
    Assert.assertEquals(
      ImmutableSet.of(new Privilege(NamespaceId.DEFAULT, Action.ADMIN), new Privilege(instance, Action.ADMIN)),
      authorizer.listPrivileges(alice)
    );
    namespaceAdmin.create(meta);
    // create should grant all privileges
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(NamespaceId.DEFAULT, Action.ADMIN),
        new Privilege(instance, Action.ADMIN),
        new Privilege(namespace, Action.ALL)
      ),
      authorizer.listPrivileges(alice)
    );
    // No authorization currently for listing and retrieving namespace
    namespaceAdmin.list();
    namespaceAdmin.get(namespace.toId());
    // revoke privileges
    authorizer.revoke(namespace);
    Assert.assertEquals(
      ImmutableSet.of(new Privilege(NamespaceId.DEFAULT, Action.ADMIN), new Privilege(instance, Action.ADMIN)),
      authorizer.listPrivileges(alice)
    );
    try {
      namespaceAdmin.deleteDatasets(namespace.toId());
      Assert.fail("Namespace delete datasets should have failed because alice's privileges on the namespace have " +
                    "been revoked");
    } catch (UnauthorizedException expected) {
      // expected
    }
    // grant privileges again
    authorizer.grant(namespace, alice, ImmutableSet.of(Action.ADMIN));
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(NamespaceId.DEFAULT, Action.ADMIN),
        new Privilege(instance, Action.ADMIN),
        new Privilege(namespace, Action.ADMIN)
      ),
      authorizer.listPrivileges(alice)
    );
    namespaceAdmin.deleteDatasets(namespace.toId());
    // deleting datasets does not revoke privileges.
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(NamespaceId.DEFAULT, Action.ADMIN),
        new Privilege(instance, Action.ADMIN),
        new Privilege(namespace, Action.ADMIN)
      ),
      authorizer.listPrivileges(alice)
    );
    NamespaceMeta updated = new NamespaceMeta.Builder(meta).setDescription("new desc").build();
    namespaceAdmin.updateProperties(namespace.toId(), updated);
    namespaceAdmin.delete(namespace.toId());
    // once the namespace has been deleted, privileges on that namespace should be revoked
    Assert.assertEquals(
      ImmutableSet.of(
        new Privilege(NamespaceId.DEFAULT, Action.ADMIN),
        new Privilege(instance, Action.ADMIN)
      ),
      authorizer.listPrivileges(alice)
    );
  }

  @AfterClass
  public static void cleanup() throws Exception {
    // we want to execute TestBase's @AfterClass before unsetting userid
    finish();
    SecurityRequestContext.setUserId(oldUser);
  }
}
