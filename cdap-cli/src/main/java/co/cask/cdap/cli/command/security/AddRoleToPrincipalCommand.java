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

package co.cask.cdap.cli.command.security;

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.client.AuthorizationClient;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Role;
import co.cask.common.cli.Arguments;
import com.google.inject.Inject;
import com.google.inject.Provider;

import java.io.PrintStream;

/**
 * Adds a {@link Role} to a {@link Principal}
 */
public class AddRoleToPrincipalCommand extends AbstractAuthCommand {

  private final Provider<AuthorizationClient> client;

  @Inject
  AddRoleToPrincipalCommand(Provider<AuthorizationClient> client, CLIConfig cliConfig) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String roleName = arguments.get("role-name");
    String principalType = arguments.get("principal-type");
    String principalName = arguments.get("principal-name");
    Principal principal = new Principal(principalName, Principal.PrincipalType.valueOf(principalType.toUpperCase()));
    client.get().addRoleToPrincipal(new Role(roleName), principal);
    output.printf("Successfully added role '%s' to '%s' '%s'\n", roleName, principalType, principalName);
  }

  @Override
  public String getPattern() {
    return String.format("add role <%s> to <%s> <%s>", ArgumentName.ROLE_NAME, ArgumentName.PRINCIPAL_TYPE,
                         ArgumentName.PRINCIPAL_NAME);
  }

  @Override
  public String getDescription() {
    return "Adds a role to a principal in authorization system for role-based access control";
  }
}
