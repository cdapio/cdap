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

package co.cask.cdap.cli.commandset;

import co.cask.cdap.cli.Categorized;
import co.cask.cdap.cli.CommandCategory;
import co.cask.cdap.cli.command.security.AddRoleToPrincipalCommand;
import co.cask.cdap.cli.command.security.CreateRoleCommand;
import co.cask.cdap.cli.command.security.DropRoleCommand;
import co.cask.cdap.cli.command.security.GrantActionCommand;
import co.cask.cdap.cli.command.security.ListPrivilegesCommand;
import co.cask.cdap.cli.command.security.ListRolesCommand;
import co.cask.cdap.cli.command.security.RemoveRoleFromPrincipalCommand;
import co.cask.cdap.cli.command.security.RevokeActionForPrincipalCommand;
import co.cask.cdap.cli.command.security.RevokeEntityCommand;
import co.cask.common.cli.Command;
import co.cask.common.cli.CommandSet;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;

/**
 * Security-related commands.
 */
public class SecurityCommands extends CommandSet<Command> implements Categorized {

  @Inject
  public SecurityCommands(Injector injector) {
    super(
      ImmutableList.<Command>builder()
        .add(injector.getInstance(GrantActionCommand.class))
        .add(injector.getInstance(RevokeActionForPrincipalCommand.class))
        .add(injector.getInstance(RevokeEntityCommand.class))
        .add(injector.getInstance(ListPrivilegesCommand.class))
        //role management commands
        .add(injector.getInstance(CreateRoleCommand.class))
        .add(injector.getInstance(DropRoleCommand.class))
        .add(injector.getInstance(ListRolesCommand.class))
        .add(injector.getInstance(AddRoleToPrincipalCommand.class))
        .add(injector.getInstance(RemoveRoleFromPrincipalCommand.class))
        .build());
  }

  @Override
  public String getCategory() {
    return CommandCategory.SECURITY.getName();
  }
}
