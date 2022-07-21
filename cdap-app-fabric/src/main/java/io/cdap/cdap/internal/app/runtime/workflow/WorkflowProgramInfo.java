/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.workflow;

import com.google.gson.Gson;
import io.cdap.cdap.api.workflow.Workflow;
import io.cdap.cdap.api.workflow.WorkflowInfo;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import org.apache.twill.api.RunId;

import java.io.Serializable;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A container which contains information for a program that runs inside a {@link Workflow}.
 */
public final class WorkflowProgramInfo implements WorkflowInfo, Serializable {

  private static final Gson GSON = new Gson();

  private final String workflowName;
  private final String workflowNodeId;
  private final String workflowRunId;
  private final String programNameInWorkflow;
  private final BasicWorkflowToken workflowToken;
  private final boolean consolidateFieldOperations;

  /**
   * Optionally creates a {@link WorkflowProgramInfo} from the given arguments. If the arguments don't contain
   * workflow information, {@code null} will be returned.
   */
  @Nullable
  public static WorkflowProgramInfo create(Arguments arguments) {
    String workflowName = arguments.getOption(ProgramOptionConstants.WORKFLOW_NAME);
    String workflowNodeId = arguments.getOption(ProgramOptionConstants.WORKFLOW_NODE_ID);
    String workflowRunId = arguments.getOption(ProgramOptionConstants.WORKFLOW_RUN_ID);
    String programNameInWorkflow = arguments.getOption(ProgramOptionConstants.PROGRAM_NAME_IN_WORKFLOW);
    String workflowToken = arguments.getOption(ProgramOptionConstants.WORKFLOW_TOKEN);
    boolean consolidateFieldOperations
            = Boolean.parseBoolean(arguments.getOption(ProgramOptionConstants.ENABLE_FIELD_LINEAGE_CONSOLIDATION));

    if (workflowName == null || workflowNodeId == null || workflowRunId == null || workflowToken == null) {
      return null;
    }

    return new WorkflowProgramInfo(workflowName, workflowNodeId, workflowRunId, programNameInWorkflow,
                                   GSON.fromJson(workflowToken, BasicWorkflowToken.class), consolidateFieldOperations);
  }

  WorkflowProgramInfo(String workflowName, String workflowNodeId, String workflowRunId, String programNameInWorkflow,
                      BasicWorkflowToken workflowToken, boolean consolidateFieldOperations) {
    this.workflowName = workflowName;
    this.workflowNodeId = workflowNodeId;
    this.workflowRunId = workflowRunId;
    this.programNameInWorkflow = programNameInWorkflow;
    this.workflowToken = workflowToken;
    this.consolidateFieldOperations = consolidateFieldOperations;
  }

  /**
   * Returns the name of the Workflow.
   */
  @Override
  public String getName() {
    return workflowName;
  }

  /**
   * Returns the node id inside the Workflow for the program.
   */
  @Override
  public String getNodeId() {
    return workflowNodeId;
  }

  /**
   * Returns the {@link RunId} of the Workflow.
   */
  @Override
  public RunId getRunId() {
    return RunIds.fromString(workflowRunId);
  }

  /**
   * Returns the name of the program used inside the Workflow.
   */
  public String getProgramNameInWorkflow() {
    return programNameInWorkflow;
  }

  /**
   * Returns the {@link BasicWorkflowToken} used in the Workflow.
   */
  public BasicWorkflowToken getWorkflowToken() {
    return workflowToken;
  }

  /**
   * Updates the metrics tags based on the information in this class.
   */
  public Map<String, String> updateMetricsTags(Map<String, String> tags) {
    tags.put(Constants.Metrics.Tag.WORKFLOW, getName());
    tags.put(Constants.Metrics.Tag.WORKFLOW_RUN_ID, getRunId().getId());
    tags.put(Constants.Metrics.Tag.NODE, getNodeId());
    return tags;
  }

  /**
   * Return {@code true} if the Workflow is going to consolidate the field operations
   * rather than emitting them from nodes running inside the Workflow, otherwise {@code false}
   * is returned.
   */
  public boolean fieldLineageConsolidationEnabled() {
    return consolidateFieldOperations;
  }
}
