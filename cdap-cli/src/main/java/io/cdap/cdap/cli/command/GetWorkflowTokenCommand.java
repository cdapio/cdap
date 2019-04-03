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

package io.cdap.cdap.cli.command;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.exception.CommandInputError;
import io.cdap.cdap.cli.util.AbstractCommand;
import io.cdap.cdap.cli.util.RowMaker;
import io.cdap.cdap.cli.util.table.Table;
import io.cdap.cdap.client.WorkflowClient;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.UnauthenticatedException;
import io.cdap.cdap.proto.WorkflowTokenDetail;
import io.cdap.cdap.proto.WorkflowTokenNodeDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.cli.Arguments;

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
    return String.format("Gets the workflow token of a workflow for a given '<%s>'",
                         ArgumentName.RUN_ID);
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
