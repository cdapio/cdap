/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.api.customaction.CustomAction;
import co.cask.cdap.api.customaction.CustomActionSpecification;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.WorkflowAction;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.internal.app.customaction.DefaultCustomActionConfigurer;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;

/**
 * Helper to create {@link WorkflowNode}
 */
final class WorkflowNodeCreator {

  private WorkflowNodeCreator() {}

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

    return new WorkflowActionNode(programName, new ScheduleProgramInfo(programType, programName));
  }

  @Deprecated
  static WorkflowNode createWorkflowCustomActionNode(WorkflowAction action) {
    Preconditions.checkArgument(action != null, "WorkflowAction is null.");
    WorkflowActionSpecification spec = DefaultWorkflowActionConfigurer.configureAction(action);
    return new WorkflowActionNode(spec.getName(), spec);
  }

  static WorkflowNode createWorkflowCustomActionNode(CustomAction action, Id.Namespace deployNamespace,
                                                     Id.Artifact artifactId, ArtifactRepository artifactRepository,
                                                     PluginInstantiator pluginInstantiator) {
    Preconditions.checkArgument(action != null, "CustomAction is null.");
    CustomActionSpecification spec = DefaultCustomActionConfigurer.configureAction(action, deployNamespace, artifactId,
                                                                                   artifactRepository,
                                                                                   pluginInstantiator);
    return new WorkflowActionNode(spec.getName(), spec);
  }
}
