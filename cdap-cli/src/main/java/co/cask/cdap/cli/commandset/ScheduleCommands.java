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

package co.cask.cdap.cli.commandset;

import co.cask.cdap.cli.Categorized;
import co.cask.cdap.cli.CommandCategory;
import co.cask.cdap.cli.command.schedule.AddTimeScheduleCommand;
import co.cask.cdap.cli.command.schedule.DeleteScheduleCommand;
import co.cask.cdap.cli.command.schedule.GetScheduleStatusCommand;
import co.cask.cdap.cli.command.schedule.ListWorkflowSchedulesCommand;
import co.cask.cdap.cli.command.schedule.ResumeScheduleCommand;
import co.cask.cdap.cli.command.schedule.SuspendScheduleCommand;
import co.cask.cdap.cli.command.schedule.UpdateTimeScheduleCommand;
import co.cask.common.cli.Command;
import co.cask.common.cli.CommandSet;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;

/**
 * Schedule commands.
 */
public class ScheduleCommands extends CommandSet<Command> implements Categorized {
  @Inject
  public ScheduleCommands(Injector injector) {
    super(
      ImmutableList.<Command>builder()
        .add(injector.getInstance(AddTimeScheduleCommand.class))
        .add(injector.getInstance(DeleteScheduleCommand.class))
        .add(injector.getInstance(GetScheduleStatusCommand.class))
        .add(injector.getInstance(ListWorkflowSchedulesCommand.class))
        .add(injector.getInstance(ResumeScheduleCommand.class))
        .add(injector.getInstance(SuspendScheduleCommand.class))
        .add(injector.getInstance(UpdateTimeScheduleCommand.class))
        .build());
  }

  @Override
  public String getCategory() {
    return CommandCategory.APPLICATION_LIFECYCLE.getName();
  }
}
