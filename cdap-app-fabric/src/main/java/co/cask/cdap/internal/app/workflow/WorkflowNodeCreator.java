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

package co.cask.cdap.internal.app.workflow;

import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.WorkflowAction;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.internal.workflow.DefaultWorkflowActionSpecification;
import com.google.common.base.Preconditions;

/**
 * Helper to create {@link WorkflowNode}
 */
class WorkflowNodeCreator {

  static WorkflowNode createWorkflowActionNode(String programName, SchedulableProgramType programType) {
    switch (programType) {
      case MAPREDUCE:
        Preconditions.checkNotNull(programName, "MapReduce name is null.");
        Preconditions.checkArgument(!programName.isEmpty(), "MapReduce name is empty.");
        break;
      case SPARK:
        Preconditions.checkNotNull(programName, "Spark name is null.");
        Preconditions.checkArgument(!programName.isEmpty(), "Spark name is empty.");
        break;
      case CUSTOM_ACTION:
        //no-op
        break;
      default:
        break;
    }

    return new WorkflowActionNode(null, new ScheduleProgramInfo(programType, programName));
  }

  static WorkflowNode createWorkflowCustomActionNode(WorkflowAction action) {
    Preconditions.checkArgument(action != null, "WorkflowAction is null.");
    WorkflowActionSpecification spec = new DefaultWorkflowActionSpecification(action);
    return new WorkflowActionNode(null, spec);
  }

}
