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
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.WorkflowClient;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.common.cli.Arguments;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Command to get the node states associated with the Workflow run.
 */
public class GetWorkflowStateCommand extends AbstractCommand {
  private final ElementType elementType;
  private final WorkflowClient workflowClient;

  public GetWorkflowStateCommand(WorkflowClient workflowClient, CLIConfig cliConfig) {
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

    ProgramRunId programRunId = cliConfig.getCurrentNamespace().app(programIdParts[0]).workflow(programIdParts[1])
      .run(arguments.get(ArgumentName.RUN_ID.toString()));

    Table table = getWorkflowNodeStates(programRunId);
    cliConfig.getTableRenderer().render(cliConfig, printStream, table);
  }

  @Override
  public String getPattern() {
    return String.format("get workflow state <%s> <%s>", elementType.getArgumentName(), ArgumentName.RUN_ID);
  }

  @Override
  public String getDescription() {
    return "Gets the state of all nodes associated with the workflow for a given run id.";
  }

  private Table getWorkflowNodeStates(ProgramRunId programRunId)
    throws UnauthenticatedException, IOException, NotFoundException, UnauthorizedException {
    Map<String, WorkflowNodeStateDetail> workflowNodeStates = workflowClient.getWorkflowNodeStates(programRunId);
    List<Map.Entry<String, WorkflowNodeStateDetail>> nodeStates = new ArrayList<>();
    nodeStates.addAll(workflowNodeStates.entrySet());
    return Table.builder()
      .setHeader("node id", "node status", "runid", "failurecause")
      .setRows(nodeStates, new RowMaker<Map.Entry<String, WorkflowNodeStateDetail>>() {
        @Override
        public List<?> makeRow(Map.Entry<String, WorkflowNodeStateDetail> object) {
          return Lists.newArrayList(object.getValue().getNodeId(), object.getValue().getNodeStatus(),
                                    object.getValue().getRunId(), object.getValue().getFailureCause());
        }
      }).build();
  }
}
