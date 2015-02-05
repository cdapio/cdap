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
import co.cask.cdap.cli.command.DeleteAppCommand;
import co.cask.cdap.cli.command.DeployAppCommand;
import co.cask.cdap.cli.command.DescribeAppCommand;
import co.cask.cdap.cli.command.ListAppsCommand;
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
        .add(injector.getInstance(ListAppsCommand.class))
        .add(injector.getInstance(DeleteAppCommand.class))
        .add(injector.getInstance(DeployAppCommand.class))
        .add(injector.getInstance(DescribeAppCommand.class))
        .build());
  }

  @Override
  public String getCategory() {
    return CommandCategory.LIFECYCLE.getName();
  }
}
