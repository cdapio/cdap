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

package co.cask.cdap.cli.command;

import co.cask.cdap.api.workflow.WorkflowToken;
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
import co.cask.cdap.proto.WorkflowTokenDetail;
import co.cask.cdap.proto.WorkflowTokenNodeDetail;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import co.cask.common.cli.Arguments;
import com.google.common.collect.Lists;
import com.google.gson.Gson;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Command to get the workflow token for a specified workflow run
 */
public class GetWorkflowTokenCommand extends AbstractCommand {
  private static final Gson GSON = new Gson();

  private final ElementType elementType;
  private final WorkflowClient workflowClient;

  public GetWorkflowTokenCommand(WorkflowClient workflowClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.elementType = ElementType.WORKFLOW;
    this.workflowClient = workflowClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String[] programIdParts = arguments.get(elementType.getArgumentName().toString()).split("\\.");
    ApplicationId appId = cliConfig.getCurrentNamespace().app(programIdParts[0]);

    if (programIdParts.length < 2) {
      throw new CommandInputError(this);
    }
    ProgramId workflowId = appId.workflow(programIdParts[1]);
    ProgramRunId runId = workflowId.run(arguments.get(ArgumentName.RUN_ID.toString()));
    WorkflowToken.Scope workflowTokenScope = null;
    if (arguments.hasArgument(ArgumentName.WORKFLOW_TOKEN_SCOPE.toString())) {
      String scope = arguments.get(ArgumentName.WORKFLOW_TOKEN_SCOPE.toString()).toUpperCase();
      workflowTokenScope = WorkflowToken.Scope.valueOf(scope);
    }
    String key = null;
    if (arguments.hasArgument(ArgumentName.WORKFLOW_TOKEN_KEY.toString())) {
      key = arguments.get(ArgumentName.WORKFLOW_TOKEN_KEY.toString());
    }

    Table table;
    if (arguments.hasArgument(ArgumentName.WORKFLOW_NODE.toString())) {
      table = getWorkflowToken(runId, workflowTokenScope, key, arguments.get(ArgumentName.WORKFLOW_NODE.toString()));
    } else {
      table = getWorkflowToken(runId, workflowTokenScope, key);
    }

    cliConfig.getTableRenderer().render(cliConfig, output, table);
  }

  @Override
  public String getPattern() {
    return String.format("get workflow token <%s> <%s> [at node <%s>] [scope <%s>] [key <%s>]",
                         elementType.getArgumentName(), ArgumentName.RUN_ID, ArgumentName.WORKFLOW_NODE,
                         ArgumentName.WORKFLOW_TOKEN_SCOPE, ArgumentName.WORKFLOW_TOKEN_KEY);
  }

  @Override
  public String getDescription() {
    return "Gets the workflow token of a workflow for a given run ID";
  }

  private Table getWorkflowToken(ProgramRunId runId, WorkflowToken.Scope workflowTokenScope, String key)
    throws UnauthenticatedException, IOException, NotFoundException, UnauthorizedException {
    WorkflowTokenDetail workflowToken = workflowClient.getWorkflowToken(runId, workflowTokenScope, key);
    List<Map.Entry<String, List<WorkflowTokenDetail.NodeValueDetail>>> tokenKeys = new ArrayList<>();
    tokenKeys.addAll(workflowToken.getTokenData().entrySet());
    return Table.builder()
      .setHeader("token key", "token value")
      .setRows(tokenKeys,
               new RowMaker<Map.Entry<String, List<WorkflowTokenDetail.NodeValueDetail>>>() {
                 @Override
                 public List<?> makeRow(Map.Entry<String, List<WorkflowTokenDetail.NodeValueDetail>> object) {
                   return Lists.newArrayList(object.getKey(), GSON.toJson(object.getValue()));
                 }
               })
      .build();
  }

  private Table getWorkflowToken(ProgramRunId runId, WorkflowToken.Scope workflowTokenScope,
                                 String key, String nodeName)
    throws UnauthenticatedException, IOException, NotFoundException, UnauthorizedException {
    WorkflowTokenNodeDetail workflowToken = workflowClient.getWorkflowTokenAtNode(runId, nodeName,
                                                                                  workflowTokenScope, key);
    List<Map.Entry<String, String>> tokenKeys = new ArrayList<>();
    tokenKeys.addAll(workflowToken.getTokenDataAtNode().entrySet());
    return Table.builder()
      .setHeader("token key", "token value")
      .setRows(tokenKeys, new RowMaker<Map.Entry<String, String>>() {
        @Override
        public List<?> makeRow(Map.Entry<String, String> object) {
          return Lists.newArrayList(object.getKey(), object.getValue());
        }
      })
      .build();
  }
}
