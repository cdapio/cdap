/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.cli.command.app.CreateAppCommand;
import co.cask.cdap.cli.command.app.DeleteAppCommand;
import co.cask.cdap.cli.command.app.DeployAppCommand;
import co.cask.cdap.cli.command.app.DescribeAppCommand;
import co.cask.cdap.cli.command.app.ListAppsCommand;
import co.cask.cdap.cli.command.app.RestartProgramsCommand;
import co.cask.cdap.cli.command.app.StartProgramsCommand;
import co.cask.cdap.cli.command.app.StatusProgramsCommand;
import co.cask.cdap.cli.command.app.StopProgramsCommand;
import co.cask.cdap.cli.command.app.UpdateAppCommand;
import co.cask.common.cli.Command;
import co.cask.common.cli.CommandSet;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;

/**
 * Application commands.
 */
public class ApplicationCommands extends CommandSet<Command> implements Categorized {

  @Inject
  public ApplicationCommands(Injector injector) {
    super(
      ImmutableList.<Command>builder()
        .add(injector.getInstance(CreateAppCommand.class))
        .add(injector.getInstance(DeleteAppCommand.class))
        .add(injector.getInstance(DeployAppCommand.class))
        .add(injector.getInstance(DescribeAppCommand.class))
        .add(injector.getInstance(ListAppsCommand.class))
        .add(injector.getInstance(RestartProgramsCommand.class))
        .add(injector.getInstance(StartProgramsCommand.class))
        .add(injector.getInstance(StatusProgramsCommand.class))
        .add(injector.getInstance(StopProgramsCommand.class))
        .add(injector.getInstance(UpdateAppCommand.class))
        .build());
  }

  @Override
  public String getCategory() {
    return CommandCategory.APPLICATION_LIFECYCLE.getName();
  }
}
