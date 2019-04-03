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

package io.cdap.cdap.cli.command;

import com.google.inject.Inject;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.client.WorkflowClient;
import io.cdap.common.cli.Command;
import io.cdap.common.cli.CommandSet;

import java.util.ArrayList;
import java.util.List;

/**
 * Contains commands for interacting with workflows.
 */
public class WorkflowCommandSet extends CommandSet<Command> {

  @Inject
  public WorkflowCommandSet(ProgramClient programClient, WorkflowClient workflowClient, CLIConfig cliConfig) {
    super(generateCommands(programClient, workflowClient, cliConfig));
  }

  private static Iterable<Command> generateCommands(ProgramClient programClient, WorkflowClient workflowClient,
                                                    CLIConfig cliConfig) {
    List<Command> commands = new ArrayList<>();
    commands.add(new GetWorkflowTokenCommand(workflowClient, cliConfig));
    commands.add(new GetWorkflowLocalDatasetsCommand(workflowClient, cliConfig));
    commands.add(new DeleteWorkflowLocalDatasetsCommand(workflowClient, cliConfig));
    commands.add(new GetWorkflowStateCommand(workflowClient, cliConfig));
    return commands;
  }
}
