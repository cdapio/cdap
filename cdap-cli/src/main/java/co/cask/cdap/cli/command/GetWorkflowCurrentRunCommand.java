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

package co.cask.cdap.cli.command;

import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.exception.CommandInputError;
import co.cask.cdap.cli.util.AbstractCommand;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.common.cli.Arguments;
import com.google.common.collect.Lists;

import java.io.PrintStream;
import java.util.List;

/**
 * Gets the current running nodes for the Workflow.
 */
public class GetWorkflowCurrentRunCommand extends AbstractCommand {

  private final ProgramClient programClient;
  private final ElementType elementType;

  protected GetWorkflowCurrentRunCommand(ElementType elementType, ProgramClient programClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.elementType = elementType;
    this.programClient = programClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String[] programIdParts = arguments.get(elementType.getArgumentName().toString()).split("\\.");
    ApplicationId appId = cliConfig.getCurrentNamespace().app(programIdParts[0]);

    List<WorkflowActionNode> nodes;
    if (elementType.getProgramType() != null) {
      if (programIdParts.length < 2) {
        throw new CommandInputError(this);
      }
      String workflowId = programIdParts[1];
      String runId = arguments.get(ArgumentName.RUN_ID.toString());

      nodes = programClient.getWorkflowCurrent(appId.workflow(workflowId), runId);
    } else {
      throw new IllegalArgumentException("Unrecognized program element type for current runs: " + elementType);
    }
    Table table = Table.builder()
      .setHeader("node id", "program name", "program type")
      .setRows(nodes, new RowMaker<WorkflowActionNode>() {
        @Override
        public List<?> makeRow(WorkflowActionNode object) {
          return Lists.newArrayList(object.getNodeId(), object.getProgram().getProgramName(),
                                    object.getProgram().getProgramType());
        }
      }).build();
    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("get workflow current <%s> <%s>", elementType.getArgumentName(), ArgumentName.RUN_ID);
  }

  @Override
  public String getDescription() {
    return String.format("Gets the currently running nodes of a workflow for a given '<%s>'", ArgumentName.RUN_ID);
  }
}
