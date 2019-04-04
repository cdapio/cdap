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

package io.cdap.cdap.cli.command.security;

import com.google.inject.Inject;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.util.AbstractAuthCommand;
import io.cdap.cdap.client.AuthorizationClient;
import io.cdap.cdap.proto.security.Role;
import io.cdap.common.cli.Arguments;

import java.io.PrintStream;

/**
 * Creates a {@link Role}
 */
public class CreateRoleCommand extends AbstractAuthCommand {

  private final AuthorizationClient client;

  @Inject
  CreateRoleCommand(AuthorizationClient client, CLIConfig cliConfig) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String roleName = arguments.get("role-name");
    client.createRole(new Role(roleName));
    output.printf("Successfully created role '%s'\n", roleName);
  }

  @Override
  public String getPattern() {
    return String.format("create role <%s>", ArgumentName.ROLE_NAME);
  }

  @Override
  public String getDescription() {
    return "Creates a role in authorization system for role-based access control";
  }
}
