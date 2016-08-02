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

package co.cask.cdap.cli;

import co.cask.cdap.StandaloneTester;
import co.cask.cdap.client.AuthorizationClient;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Role;
import co.cask.cdap.security.authorization.InMemoryAuthorizer;
import co.cask.cdap.security.server.BasicAuthenticationHandler;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.common.cli.CLI;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests authorization CLI commands. These tests are in their own class because they need authorization enabled.
 */
public class AuthorizationCLITest extends CLITestBase {

  /**
   * An {@link ExternalResource} that wraps a {@link TemporaryFolder} and {@link StandaloneTester} to execute them in
   * a chain.
   */
  private static final class StandaloneTesterWithAuthorization extends ExternalResource {
    private final TemporaryFolder tmpFolder = new TemporaryFolder();
    private StandaloneTester standaloneTester;

    @Override
    public Statement apply(final Statement base, final Description description) {
      // Apply the TemporaryFolder on a Statement that creates a StandaloneTester and applies on base
      return tmpFolder.apply(new Statement() {
        @Override
        public void evaluate() throws Throwable {
          standaloneTester = new StandaloneTester(getAuthConfigs(tmpFolder.newFolder()));
          standaloneTester.apply(base, description).evaluate();
        }
      }, description);
    }

    /**
     * Return the base URI of Standalone for use in tests.
     */
    public URI getBaseURI() {
      return standaloneTester.getBaseURI();
    }

    private static String[] getAuthConfigs(File tmpDir) throws IOException {
      LocationFactory locationFactory = new LocalLocationFactory(tmpDir);
      Location authExtensionJar = AppJarHelper.createDeploymentJar(locationFactory, InMemoryAuthorizer.class);
      return new String[] {
        // We want to enable security, but bypass it for only testing authorization commands
        Constants.Security.ENABLED, "true",
        Constants.Security.AUTH_HANDLER_CLASS, BasicAuthenticationHandler.class.getName(),
        Constants.Security.Router.BYPASS_AUTHENTICATION_REGEX, ".*",
        Constants.Security.Authorization.ENABLED, "true",
        Constants.Security.Authorization.CACHE_ENABLED, "false",
        Constants.Security.Authorization.EXTENSION_JAR_PATH, authExtensionJar.toURI().getPath(),
        // Bypass authorization enforcement for grant/revoke operations in this test. Authorization enforcement for
        // grant/revoke is tested in AuthorizationHandlerTest
        Constants.Security.Authorization.EXTENSION_CONFIG_PREFIX + "superusers", "*",
        // we only want to test authorization, but we don't specify principal/keytab, so disable kerberos
        Constants.Security.KERBEROS_ENABLED, "false",
        Constants.Explore.EXPLORE_ENABLED, "false"
      };
    }
  }

  @ClassRule
  public static final StandaloneTesterWithAuthorization AUTH_STANDALONE = new StandaloneTesterWithAuthorization();

  private static final InstanceId INSTANCE_ID = new InstanceId("cdap");

  private static CLI cli;
  private static AuthorizationClient authorizationClient;

  @BeforeClass
  public static void setup() throws Exception {
    CLIConfig cliConfig = createCLIConfig(AUTH_STANDALONE.getBaseURI());
    LaunchOptions launchOptions = new LaunchOptions(LaunchOptions.DEFAULT.getUri(), true, true, false);
    CLIMain cliMain = new CLIMain(launchOptions, cliConfig);
    cli = cliMain.getCLI();
    testCommandOutputContains(cli, "connect " + AUTH_STANDALONE.getBaseURI(), "Successfully connected");
    authorizationClient = new AuthorizationClient(cliConfig.getClientConfig());
    // Grant the privileges on the instance first. This is so that the current user can create a namespace.
    // This needs to be done using the client because in these tests, it is impossible to set the
    // SecurityRequestContext to a non-null value. Having a null user name is fine, but when it is used as null via a
    // CLI command, the null is serialized to the String "null" which causes issues during enforcement, when the user
    // is received as null, and not the String "null".
    authorizationClient.grant(INSTANCE_ID, SecurityRequestContext.toPrincipal(), Collections.singleton(Action.ADMIN));
  }

  @Test
  public void testAuthorizationCLI() throws Exception {
    Role role = new Role("admins");
    Principal principal = new Principal("spiderman", Principal.PrincipalType.USER);

    NamespaceId namespaceId = new NamespaceId("ns1");

    testCommandOutputContains(cli, String.format("create namespace %s", namespaceId.getNamespace()),
                              String.format("Namespace '%s' created successfully", namespaceId.getNamespace()));

    // test creating role
    testCommandOutputContains(cli, "create role " + role.getName(), String.format("Successfully created role '%s'",
                                                                                  role.getName()));

    // test add role to principal
    testCommandOutputContains(cli, String.format("add role %s to %s %s", role.getName(), principal.getType(),
                                                 principal.getName()),
                              String.format("Successfully added role '%s' to '%s' '%s'", role.getName(),
                                            principal.getType(), principal.getName()));

    // test listing all roles
    String output = getCommandOutput(cli, "list roles");
    List<String> lines = Arrays.asList(output.split("\\r?\\n"));
    Assert.assertEquals(2, lines.size());
    Assert.assertEquals(role.getName(), lines.get(1)); // 0 is just the table headers

    // test listing roles for a principal
    output = getCommandOutput(cli, String.format("list roles for %s %s", principal.getType(), principal.getName()));
    lines = Arrays.asList(output.split("\\r?\\n"));
    Assert.assertEquals(2, lines.size());
    Assert.assertEquals(role.getName(), lines.get(1));

    // test grant action. also tests case insensitivity of Action and Principal.PrincipalType
    testCommandOutputContains(cli, String.format("grant actions %s on entity %s to %s %s",
                                                 Action.READ.name().toLowerCase(), namespaceId.toString(),
                                                 principal.getType().name().toLowerCase(), principal.getName()),
                              String.format("Successfully granted action(s) '%s' on entity '%s' to %s '%s'",
                                            Action.READ, namespaceId.toString(), principal.getType(),
                                            principal.getName()));

    // test listing privilege
    output = getCommandOutput(cli, String.format("list privileges for %s %s", principal.getType(),
                                                 principal.getName()));
    lines = Arrays.asList(output.split("\\r?\\n"));
    Assert.assertEquals(2, lines.size());
    Assert.assertArrayEquals(new String[]{namespaceId.toString(), Action.READ.name()}, lines.get(1).split(","));


    // test revoke actions
    testCommandOutputContains(cli, String.format("revoke actions %s on entity %s from %s %s", Action.READ,
                                                 namespaceId.toString(), principal.getType(), principal.getName()),
                              String.format("Successfully revoked action(s) '%s' on entity '%s' for %s '%s'",
                                            Action.READ, namespaceId.toString(), principal.getType(),
                                            principal.getName()));

    // grant and perform revoke on the entity
    testCommandOutputContains(cli, String.format("grant actions %s on entity %s to %s %s", Action.READ,
                                                 namespaceId.toString(), principal.getType(), principal.getName()),
                              String.format("Successfully granted action(s) '%s' on entity '%s' to %s '%s'",
                                            Action.READ, namespaceId.toString(), principal.getType(),
                                            principal.getName()));

    testCommandOutputContains(cli, String.format("revoke all on entity %s ", namespaceId.toString()),
                              String.format("Successfully revoked all actions on entity '%s' for all principals",
                                            namespaceId.toString()));


    // test remove role from principal
    testCommandOutputContains(cli, String.format("remove role %s from %s %s", role.getName(), principal.getType(),
                                                 principal.getName()),
                              String.format("Successfully removed role '%s' from %s '%s'", role.getName(),
                                            principal.getType(), principal.getName()));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    authorizationClient.revoke(INSTANCE_ID);
  }
}
