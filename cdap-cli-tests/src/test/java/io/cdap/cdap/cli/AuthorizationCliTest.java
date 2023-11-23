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

package io.cdap.cdap.cli;

import io.cdap.cdap.StandaloneTester;
import io.cdap.cdap.client.AuthorizationClient;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.ApplicationPermission;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.PermissionType;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.Role;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.authorization.InMemoryAccessControllerV2;
import io.cdap.cdap.security.server.BasicAuthenticationHandler;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.common.cli.CLI;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

/**
 * Tests authorization CLI commands. These tests are in their own class because they need authorization enabled.
 */
public class AuthorizationCliTest {

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
    public URI getBaseUri() {
      return standaloneTester.getBaseURI();
    }

    private static String[] getAuthConfigs(File tmpDir) throws IOException {
      LocationFactory locationFactory = new LocalLocationFactory(tmpDir);
      Location realmfile = locationFactory.create("realmfile");
      realmfile.createNew();
      File file = new File(AppJarHelper.createDeploymentJar(locationFactory,
                                                            InMemoryAccessControllerV2.AuthorizableEntityId.class)
                             .toURI());
      Location authExtensionJar = AppJarHelper.createDeploymentJar(
        locationFactory, InMemoryAccessControllerV2.class, file);
      return new String[] {
        // We want to enable security, but bypass it for only testing authorization commands
        Constants.Security.ENABLED, "true",
        Constants.Security.AUTH_HANDLER_CLASS, BasicAuthenticationHandler.class.getName(),
        // we bypass authentication, but BasicAuthenticationHandler requires a valid file upon initialization
        Constants.Security.BASIC_REALM_FILE, realmfile.toString(),
        Constants.Security.Router.BYPASS_AUTHENTICATION_REGEX, ".*",
        Constants.Security.Authorization.ENABLED, "true",
        Constants.Security.Authorization.CACHE_MAX_ENTRIES, "0",
        Constants.Security.Authorization.EXTENSION_JAR_PATH, authExtensionJar.toURI().getPath(),
        // Bypass authorization enforcement for grant/revoke operations in this test. Authorization enforcement for
        // grant/revoke is tested in AuthorizationHandlerTest
        Constants.Security.Authorization.EXTENSION_CONFIG_PREFIX + "superusers", "*",
        // we only want to test authorization, but we don't specify principal/keytab, so disable kerberos
        Constants.Security.KERBEROS_ENABLED, "false"
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
    CLIConfig cliConfig = CLITestBase.createCLIConfig(AUTH_STANDALONE.getBaseUri());
    LaunchOptions launchOptions = new LaunchOptions(LaunchOptions.DEFAULT.getUri(), true, true, false);
    CLIMain cliMain = new CLIMain(launchOptions, cliConfig);
    cli = cliMain.getCLI();
    CLITestBase.testCommandOutputContains(cli, "connect " + AUTH_STANDALONE.getBaseUri(), "Successfully connected");
    authorizationClient = new AuthorizationClient(cliConfig.getClientConfig());
    // Grant the privileges on the instance first. This is so that the current user can create a namespace.
    // This needs to be done using the client because in these tests, it is impossible to set the
    // SecurityRequestContext to a non-null value. Having a null user name is fine, but when it is used as null via a
    // CLI command, the null is serialized to the String "null" which causes issues during enforcement, when the user
    // is received as null, and not the String "null".
    authorizationClient.grant(Authorizable.fromEntityId(INSTANCE_ID),
                              SecurityRequestContext.toPrincipal(), Collections.singleton(StandardPermission.UPDATE));
  }

  @Test
  public void testAuthorizationCli() throws Exception {
    Role role = new Role("admins");
    Principal principal = new Principal("spiderman", Principal.PrincipalType.USER);

    NamespaceId namespaceId = new NamespaceId("ns1");

    CLITestBase.testCommandOutputContains(cli, String.format("create namespace %s", namespaceId.getNamespace()),
                                          String.format("Namespace '%s' created successfully",
                                                        namespaceId.getNamespace()));

    // test creating role
    CLITestBase.testCommandOutputContains(cli, "create role " + role.getName(),
                                          String.format("Successfully created role '%s'",
                                                        role.getName()));

    // test add role to principal
    CLITestBase.testCommandOutputContains(cli, String.format("add role %s to %s %s", role.getName(),
                                                             principal.getType(),
                                                             principal.getName()),
                                          String.format("Successfully added role '%s' to '%s' '%s'", role.getName(),
                                                        principal.getType(), principal.getName()));

    // test listing all roles
    String output = CLITestBase.getCommandOutput(cli, "list roles");
    List<String> lines = Arrays.asList(output.split("\\r?\\n"));
    Assert.assertEquals(2, lines.size());
    Assert.assertEquals(role.getName(), lines.get(1)); // 0 is just the table headers

    // test listing roles for a principal
    output = CLITestBase.getCommandOutput(cli, String.format("list roles for %s %s", principal.getType(),
                                                             principal.getName()));
    lines = Arrays.asList(output.split("\\r?\\n"));
    Assert.assertEquals(2, lines.size());
    Assert.assertEquals(role.getName(), lines.get(1));

    // test grant permission. also tests case insensitivity of Permission and Principal.PrincipalType
    CLITestBase.testCommandOutputContains(cli, String.format("grant permissions %s on entity %s to %s %s",
                                                             StandardPermission.GET.name().toLowerCase(),
                                                             namespaceId.toString(),
                                                             principal.getType().name().toLowerCase(),
                                                             principal.getName()),
                                          String.format(
                                            "Successfully granted permission(s) '%s' on entity '%s' to %s '%s'",
                                            StandardPermission.GET, namespaceId.toString(),
                                            principal.getType(), principal.getName()));

    // test grant permission for application permission (dotted syntax)
    CLITestBase.testCommandOutputContains(cli, String.format("grant permissions %s.%s on entity %s to %s %s",
                                                             PermissionType.APPLICATION.name().toLowerCase(),
                                                             ApplicationPermission.EXECUTE.name().toLowerCase(),
                                                             namespaceId.toString(),
                                                             principal.getType().name().toLowerCase(),
                                                             principal.getName()),
                                          String.format(
                                            "Successfully granted permission(s) '%s' on entity '%s' to %s '%s'",
                                            ApplicationPermission.EXECUTE,
                                            namespaceId.toString(), principal.getType(),
                                            principal.getName()));

    // test listing privilege
    output = CLITestBase.getCommandOutput(cli, String.format("list privileges for %s %s", principal.getType(),
                                                             principal.getName()));
    lines = Stream.of(output.split("\\r?\\n")).sorted().collect(Collectors.toList());
    Assert.assertEquals(3, lines.size());
    Assert.assertArrayEquals(new String[]{namespaceId.toString(), ApplicationPermission.EXECUTE.name()},
                             lines.get(1).split(","));
    Assert.assertArrayEquals(new String[]{namespaceId.toString(), StandardPermission.GET.name()},
                             lines.get(2).split(","));


    // test revoke permissions
    CLITestBase.testCommandOutputContains(cli, String.format("revoke permissions %s on entity %s from %s %s",
                                                             StandardPermission.GET,
                                                             namespaceId.toString(), principal.getType(),
                                                             principal.getName()),
                                          String.format(
                                            "Successfully revoked permission(s) '%s' on entity '%s' for %s '%s'",
                                            StandardPermission.GET, namespaceId.toString(), principal.getType(),
                                            principal.getName()));

    // grant and perform revoke on the entity
    CLITestBase.testCommandOutputContains(cli, String.format("grant permissions %s on entity %s to %s %s",
                                                             StandardPermission.GET,
                                                             namespaceId.toString(), principal.getType(),
                                                             principal.getName()),
                                          String.format(
                                            "Successfully granted permission(s) '%s' on entity '%s' to %s '%s'",
                                            StandardPermission.GET, namespaceId.toString(), principal.getType(),
                                            principal.getName()));

    CLITestBase.testCommandOutputContains(cli, String.format("revoke all on entity %s ", namespaceId.toString()),
                                          String.format(
                                            "Successfully revoked all permissions on entity '%s' for all principals",
                                            namespaceId.toString()));


    // test remove role from principal
    CLITestBase.testCommandOutputContains(cli, String.format("remove role %s from %s %s", role.getName(),
                                                             principal.getType(),
                                                             principal.getName()),
                                          String.format("Successfully removed role '%s' from %s '%s'", role.getName(),
                                                        principal.getType(), principal.getName()));

    // test remove role (which doesn't exist) from principal
    Role nonexistentRole = new Role("nonexistent_role");
    CLITestBase.testCommandOutputContains(cli, String.format("remove role %s from %s %s", nonexistentRole.getName(),
                                                             principal.getType(), principal.getName()),
                                          String.format("Error: %s not found", nonexistentRole));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    authorizationClient.revoke(Authorizable.fromEntityId(INSTANCE_ID));
  }
}
