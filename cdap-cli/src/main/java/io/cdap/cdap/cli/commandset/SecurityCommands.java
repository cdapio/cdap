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

package io.cdap.cdap.cli.commandset;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.cli.Categorized;
import io.cdap.cdap.cli.CommandCategory;
import io.cdap.cdap.cli.command.security.AddRoleToPrincipalCommand;
import io.cdap.cdap.cli.command.security.CreateRoleCommand;
import io.cdap.cdap.cli.command.security.DropRoleCommand;
import io.cdap.cdap.cli.command.security.GrantActionCommand;
import io.cdap.cdap.cli.command.security.ListPrivilegesCommand;
import io.cdap.cdap.cli.command.security.ListRolesCommand;
import io.cdap.cdap.cli.command.security.RemoveRoleFromPrincipalCommand;
import io.cdap.cdap.cli.command.security.RevokeActionForPrincipalCommand;
import io.cdap.cdap.cli.command.security.RevokeEntityCommand;
import io.cdap.common.cli.Command;
import io.cdap.common.cli.CommandSet;

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
