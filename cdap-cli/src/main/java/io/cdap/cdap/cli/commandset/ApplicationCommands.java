/*
 * Copyright © 2015-2017 Cask Data, Inc.
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
import io.cdap.cdap.cli.command.app.CreateAppCommand;
import io.cdap.cdap.cli.command.app.DeleteAppCommand;
import io.cdap.cdap.cli.command.app.DeployAppCommand;
import io.cdap.cdap.cli.command.app.DeployAppWithConfigFileCommand;
import io.cdap.cdap.cli.command.app.DescribeAppCommand;
import io.cdap.cdap.cli.command.app.ListAppVersionsCommand;
import io.cdap.cdap.cli.command.app.ListAppsCommand;
import io.cdap.cdap.cli.command.app.RestartProgramsCommand;
import io.cdap.cdap.cli.command.app.StartProgramsCommand;
import io.cdap.cdap.cli.command.app.StatusProgramsCommand;
import io.cdap.cdap.cli.command.app.StopProgramsCommand;
import io.cdap.cdap.cli.command.app.UpdateAppCommand;
import io.cdap.common.cli.Command;
import io.cdap.common.cli.CommandSet;

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
        .add(injector.getInstance(DeployAppWithConfigFileCommand.class))
        .add(injector.getInstance(DeployAppCommand.class))
        .add(injector.getInstance(DescribeAppCommand.class))
        .add(injector.getInstance(ListAppsCommand.class))
        .add(injector.getInstance(ListAppVersionsCommand.class))
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
