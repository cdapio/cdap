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
 * Revoke command base class
 */
public abstract class RevokePermissionCommand extends AbstractAuthCommand {

  private final AuthorizationClient client;

  @Inject
  RevokePermissionCommand(AuthorizationClient client, CLIConfig cliConfig) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    Authorizable authorizable = Authorizable.fromString(
        arguments.get(ArgumentName.ENTITY.toString()));
    String principalName = arguments.getOptional("principal-name", null);
    String type = arguments.getOptional("principal-type", null);
    Principal.PrincipalType principalType =
        type != null ? Principal.PrincipalType.valueOf(type.toUpperCase()) : null;
    Principal principal = type != null ? new Principal(principalName, principalType) : null;
    String permissionsString = arguments.getOptional("permissions", null);
    Set<Permission> permissions =
        permissionsString == null ? null : PERMISSION_STRING_TO_SET.apply(permissionsString);

    client.revoke(authorizable, principal, permissions);
    if (principal == null && permissions == null) {
      // Revoked all permissions for all principals on the entity
      output.printf("Successfully revoked all permissions on entity '%s' for all principals",
          authorizable.toString());
    } else {
      // currently, the CLI only supports 2 scenarios:
      // 1. both permissions and principal are null - supported in the if block.
      // 2. both permissions and principal are non-null - supported here. So it should be ok to have preconditions here
      // to enforce that both are non-null. In fact, if only one of them is null, the CLI will fail to parse the
      // command.
      Preconditions.checkNotNull(permissions,
          "Permissions cannot be null when principal is not null in the revoke command");
      Preconditions.checkNotNull(principal,
          "Principal cannot be null when permissions is not null in the revoke command");
      output.printf("Successfully revoked permission(s) '%s' on entity '%s' for %s '%s'\n",
          Joiner.on(",").join(permissions), authorizable.toString(),
          principal.getType(), principal.getName());
    }
  }
}
