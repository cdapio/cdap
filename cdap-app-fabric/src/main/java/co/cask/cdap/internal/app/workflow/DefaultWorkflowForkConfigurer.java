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
import co.cask.cdap.api.workflow.WorkflowAction;
import co.cask.cdap.api.workflow.WorkflowForkConfigurer;
import co.cask.cdap.api.workflow.WorkflowForkSpecification;
import co.cask.cdap.api.workflow.WorkflowNode;
import com.clearspring.analytics.util.Lists;

import java.util.List;

import javax.annotation.Nullable;

/**
 * Default implementation of the {@link WorkflowForkConfigurer}
 */
public class DefaultWorkflowForkConfigurer implements WorkflowForkConfigurer {

  private final WorkflowForkConfigurer parentForkConfigurer;
  private final DefaultWorkflowConfigurer workflowConfigurer;
  private final String forkId;

  private final List<List<WorkflowNode>> branches = Lists.newArrayList();
  private List<WorkflowNode> currentBranch;

  public DefaultWorkflowForkConfigurer(DefaultWorkflowConfigurer workflowConfigurer,
                                       @Nullable WorkflowForkConfigurer parentForkConfigurer, String forkId) {
    this.parentForkConfigurer = parentForkConfigurer;
    this.workflowConfigurer = workflowConfigurer;
    currentBranch = Lists.newArrayList();
    this.forkId = forkId;
  }

  @Override
  public WorkflowForkConfigurer addMapReduce(String mapReduce) {
    currentBranch.add(workflowConfigurer.getWorkflowActionNode(mapReduce, SchedulableProgramType.MAPREDUCE));
    return this;
  }

  @Override
  public WorkflowForkConfigurer addSpark(String spark) {
    currentBranch.add(workflowConfigurer.getWorkflowActionNode(spark, SchedulableProgramType.CUSTOM_ACTION));
    return this;
  }

  @Override
  public WorkflowForkConfigurer addAction(WorkflowAction action) {
    currentBranch.add(workflowConfigurer.getWorkflowCustomActionNode(action));
    return this;
  }

  @Override
  public WorkflowForkConfigurer addFork() {
    return workflowConfigurer.getWorkflowForkConfigurer(this);
  }

  @Override
  public WorkflowForkConfigurer also() {
    branches.add(currentBranch);
    currentBranch = Lists.newArrayList();
    return this;
  }

  @Nullable
  @Override
  public WorkflowForkConfigurer join() {
    workflowConfigurer.addWorkflowForkSpecification(forkId, new WorkflowForkSpecification(branches));
    return parentForkConfigurer;
  }
}
