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
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.AuthorizationClient;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.proto.security.Role;
import co.cask.common.cli.Arguments;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.util.List;
import java.util.Set;

/**
 * Lists {@link Role}
 */
public class ListRolesCommand extends AbstractAuthCommand {

  private final AuthorizationClient client;

  @Inject
  ListRolesCommand(AuthorizationClient client, CLIConfig cliConfig) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String principalType = arguments.getOptional(ArgumentName.PRINCIPAL_TYPE.toString());
    String principalName = arguments.getOptional(ArgumentName.PRINCIPAL_NAME.toString());

    Set<Role> roles;
    if (!(Strings.isNullOrEmpty(principalType) && Strings.isNullOrEmpty(principalName))) {
      roles = client.listRoles(new Principal(principalName,
                                             Principal.PrincipalType.valueOf(principalType.toUpperCase())));
    } else {
      roles = client.listAllRoles();
    }

    Table table = Table.builder()
      .setHeader("Role")
      .setRows(Lists.newArrayList(roles), new RowMaker<Role>() {
        @Override
        public List<?> makeRow(Role role) {
          return Lists.newArrayList(role.getName());
        }
      }).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("list roles [for <%s> <%s>]", ArgumentName.PRINCIPAL_TYPE, ArgumentName.PRINCIPAL_NAME);
  }

  @Override
  public String getDescription() {
    return "Lists all roles, optionally for a particular principal in an authorization system for role-based " +
      "access control";
  }
}
