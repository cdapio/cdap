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
import io.cdap.cdap.cli.command.GetProgramInstancesCommandSet;
import io.cdap.cdap.cli.command.GetProgramLiveInfoCommandSet;
import io.cdap.cdap.cli.command.GetProgramLogsCommandSet;
import io.cdap.cdap.cli.command.GetProgramRunsCommandSet;
import io.cdap.cdap.cli.command.GetProgramRuntimeArgsCommandSet;
import io.cdap.cdap.cli.command.GetProgramStatusCommandSet;
import io.cdap.cdap.cli.command.ListAllProgramsCommand;
import io.cdap.cdap.cli.command.ListProgramsCommandSet;
import io.cdap.cdap.cli.command.SetProgramInstancesCommandSet;
import io.cdap.cdap.cli.command.SetProgramRuntimeArgsCommandSet;
import io.cdap.cdap.cli.command.StartProgramCommandSet;
import io.cdap.cdap.cli.command.StopProgramCommandSet;
import io.cdap.cdap.cli.command.WorkflowCommandSet;
import io.cdap.common.cli.Command;
import io.cdap.common.cli.CommandSet;

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
        .add(injector.getInstance(WorkflowCommandSet.class))
        .build());
  }

  @Override
  public String getCategory() {
    return CommandCategory.APPLICATION_LIFECYCLE.getName();
  }
}
