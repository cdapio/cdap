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

package io.cdap.cdap.internal.app.workflow;

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.customaction.CustomAction;
import io.cdap.cdap.api.customaction.CustomActionSpecification;
import io.cdap.cdap.api.schedule.SchedulableProgramType;
import io.cdap.cdap.api.workflow.ScheduleProgramInfo;
import io.cdap.cdap.api.workflow.WorkflowActionNode;
import io.cdap.cdap.api.workflow.WorkflowNode;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.customaction.DefaultCustomActionConfigurer;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;

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
