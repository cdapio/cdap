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
import co.cask.cdap.cli.command.GetProgramInstancesCommandSet;
import co.cask.cdap.cli.command.GetProgramLiveInfoCommandSet;
import co.cask.cdap.cli.command.GetProgramLogsCommandSet;
import co.cask.cdap.cli.command.GetProgramRunsCommandSet;
import co.cask.cdap.cli.command.GetProgramRuntimeArgsCommandSet;
import co.cask.cdap.cli.command.GetProgramStatusCommandSet;
import co.cask.cdap.cli.command.ListAllProgramsCommand;
import co.cask.cdap.cli.command.ListProgramsCommandSet;
import co.cask.cdap.cli.command.SetProgramInstancesCommandSet;
import co.cask.cdap.cli.command.SetProgramRuntimeArgsCommandSet;
import co.cask.cdap.cli.command.StartProgramCommandSet;
import co.cask.cdap.cli.command.StopProgramCommandSet;
import co.cask.common.cli.Command;
import co.cask.common.cli.CommandSet;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;

/**
 * Program commands.
 */
public class ProgramCommands extends CommandSet<Command> implements Categorized {

  @Inject
  public ProgramCommands(Injector injector) {
    super(
      ImmutableList.<Command>builder()
        .add(injector.getInstance(ListAllProgramsCommand.class))
        .build(),
      ImmutableList.<CommandSet<Command>>builder()
        .add(injector.getInstance(ListProgramsCommandSet.class))
        .add(injector.getInstance(GetProgramRunsCommandSet.class))
        .add(injector.getInstance(GetProgramInstancesCommandSet.class))
        .add(injector.getInstance(GetProgramLiveInfoCommandSet.class))
        .add(injector.getInstance(GetProgramLogsCommandSet.class))
        .add(injector.getInstance(GetProgramStatusCommandSet.class))
        .add(injector.getInstance(GetProgramRuntimeArgsCommandSet.class))
        .add(injector.getInstance(SetProgramRuntimeArgsCommandSet.class))
        .add(injector.getInstance(SetProgramInstancesCommandSet.class))
        .add(injector.getInstance(StartProgramCommandSet.class))
        .add(injector.getInstance(StopProgramCommandSet.class))
        .build());
  }

  @Override
  public String getCategory() {
    return CommandCategory.LIFECYCLE.getName();
  }
}
