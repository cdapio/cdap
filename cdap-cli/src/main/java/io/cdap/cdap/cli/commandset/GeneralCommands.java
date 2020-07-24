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

package io.cdap.cdap.cli.commandset;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.cli.Categorized;
import io.cdap.cdap.cli.CommandCategory;
import io.cdap.cdap.cli.command.ConnectCommand;
import io.cdap.cdap.cli.command.ConnectLinkCommand;
import io.cdap.cdap.cli.command.system.ExitCommand;
import io.cdap.cdap.cli.command.system.QuitCommand;
import io.cdap.cdap.cli.command.system.RenderAsCommand;
import io.cdap.cdap.cli.command.system.VersionCommand;
import io.cdap.common.cli.Command;
import io.cdap.common.cli.CommandSet;

/**
 * General commands.
 */
public class GeneralCommands extends CommandSet<Command> implements Categorized {

  @Inject
  public GeneralCommands(Injector injector) {
    super(
      ImmutableList.<Command>builder()
        .add(injector.getInstance(VersionCommand.class))
        .add(injector.getInstance(ExitCommand.class))
        .add(injector.getInstance(QuitCommand.class))
        .add(injector.getInstance(ConnectCommand.class))
        .add(injector.getInstance(ConnectLinkCommand.class))
        .add(injector.getInstance(RenderAsCommand.class))
        .build());
  }

  @Override
  public String getCategory() {
    return CommandCategory.GENERAL.getName();
  }
}
