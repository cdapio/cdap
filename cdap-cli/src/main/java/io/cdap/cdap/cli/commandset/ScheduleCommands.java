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
import io.cdap.cdap.cli.command.schedule.AddTimeScheduleCommand;
import io.cdap.cdap.cli.command.schedule.DeleteScheduleCommand;
import io.cdap.cdap.cli.command.schedule.GetScheduleStatusCommand;
import io.cdap.cdap.cli.command.schedule.ListWorkflowSchedulesCommand;
import io.cdap.cdap.cli.command.schedule.ResumeScheduleCommand;
import io.cdap.cdap.cli.command.schedule.SuspendScheduleCommand;
import io.cdap.cdap.cli.command.schedule.UpdateTimeScheduleCommand;
import io.cdap.common.cli.Command;
import io.cdap.common.cli.CommandSet;

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
