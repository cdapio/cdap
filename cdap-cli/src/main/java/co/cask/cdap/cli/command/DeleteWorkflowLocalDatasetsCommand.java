/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.cli.command;

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.exception.CommandInputError;
import co.cask.cdap.cli.util.AbstractCommand;
import co.cask.cdap.client.WorkflowClient;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.common.cli.Arguments;

import java.io.PrintStream;

/**
 * Command to delete the local datasets associated with the Workflow run.
 */
public class DeleteWorkflowLocalDatasetsCommand extends AbstractCommand {
  private final ElementType elementType;
  private final WorkflowClient workflowClient;

  public DeleteWorkflowLocalDatasetsCommand(WorkflowClient workflowClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.elementType = ElementType.WORKFLOW;
    this.workflowClient = workflowClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream printStream) throws Exception {
    String[] programIdParts = arguments.get(elementType.getArgumentName().toString()).split("\\.");

    if (programIdParts.length < 2) {
      throw new CommandInputError(this);
    }

    ProgramRunId programRunId = new ProgramRunId(cliConfig.getCurrentNamespace().getId(), programIdParts[0],
                                                 ProgramType.WORKFLOW, programIdParts[1],
                                                 arguments.get(ArgumentName.RUN_ID.toString()));

    workflowClient.deleteWorkflowLocalDatasets(programRunId);
    printStream.printf("Successfully deleted local datasets associated with the workflow run.");
  }

  @Override
  public String getPattern() {
    return String.format("delete workflow local datasets <%s> <%s>", elementType.getArgumentName(),
                         ArgumentName.RUN_ID);
  }

  @Override
  public String getDescription() {
    return "Deletes the local datasets associated with the workflow for a given run id.";
  }
}
