/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.util.AbstractAuthCommand;
import io.cdap.cdap.client.AuthorizationClient;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Permission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.common.cli.Arguments;
import java.io.PrintStream;
import java.util.Set;

/**
 * Grants a user permission to perform certain actions on an auhorizable.
 */
public class GrantPermissionCommand extends AbstractAuthCommand {

  private final AuthorizationClient client;

  @Inject
  GrantPermissionCommand(AuthorizationClient client, CLIConfig cliConfig) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    Authorizable authorizable = Authorizable.fromString(
        arguments.get(ArgumentName.ENTITY.toString()));
    String principalName = arguments.get("principal-name");
    Principal.PrincipalType principalType =
        Principal.PrincipalType.valueOf(arguments.get("principal-type").toUpperCase());
    Principal principal = new Principal(principalName, principalType);
    Set<Permission> permissions = PERMISSION_STRING_TO_SET.apply(arguments.get("permissions"));
    // permissions is not an optional argument so should never be null
    Preconditions.checkNotNull(permissions, "Permissions can never be null in the grant command.");

    client.grant(authorizable, principal, permissions);
    output.printf("Successfully granted permission(s) '%s' on entity '%s' to %s '%s'\n",
        Joiner.on(",").join(permissions), authorizable.toString(), principal.getType(),
        principal.getName());
  }

  @Override
  public String getPattern() {
    return String.format("grant permissions <permissions> on entity <%s> to <%s> <%s>",
        ArgumentName.ENTITY,
        ArgumentName.PRINCIPAL_TYPE, ArgumentName.PRINCIPAL_NAME);
  }

  @Override
  public String getDescription() {
    return String.format(
        "Grants a principal permissions to perform certain actions on an authorizable. %s %s",
        ArgumentName.ENTITY_DESCRIPTION_PERMISSIONS, ArgumentName.ENTITY_DESCRIPTION_ALL_STRING);
  }
}
