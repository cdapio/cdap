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
import co.cask.cdap.api.workflow.WorkflowConfigurer;
import co.cask.cdap.api.workflow.WorkflowForkBranch;
import co.cask.cdap.api.workflow.WorkflowForkConfigurer;
import co.cask.cdap.api.workflow.WorkflowForkNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import com.clearspring.analytics.util.Lists;

import java.util.List;

import javax.annotation.Nullable;

/**
 * Default implementation of the {@link WorkflowForkConfigurer}
 * @param <T>
 */
public class DefaultWorkflowForkConfigurer<T> implements WorkflowForkConfigurer<T> {

  private final WorkflowForkConfigurer<T> parentForkConfigurer;
  private final WorkflowConfigurer workflowConfigurer;
  private final String forkNodeId;

  private final List<WorkflowForkBranch> branches = Lists.newArrayList();
  private List<WorkflowNode> currentBranch;

  public DefaultWorkflowForkConfigurer(WorkflowConfigurer workflowConfigurer,
                                       @Nullable WorkflowForkConfigurer<T> parentForkConfigurer,
                                       String forkNodeId) {
    this.parentForkConfigurer = parentForkConfigurer;
    this.workflowConfigurer = workflowConfigurer;
    currentBranch = Lists.newArrayList();
    this.forkNodeId = forkNodeId;
  }

  @Override
  public WorkflowForkConfigurer<T> addMapReduce(String mapReduce) {
    currentBranch.add(((DefaultWorkflowConfigurer) workflowConfigurer).getWorkflowActionNode
      (mapReduce, SchedulableProgramType.MAPREDUCE));
    return this;
  }

  @Override
  public WorkflowForkConfigurer<T> addSpark(String spark) {
    currentBranch.add(((DefaultWorkflowConfigurer) workflowConfigurer).getWorkflowActionNode
      (spark, SchedulableProgramType.CUSTOM_ACTION));
    return this;
  }

  @Override
  public WorkflowForkConfigurer<T> addAction(WorkflowAction action) {
    currentBranch.add(((DefaultWorkflowConfigurer) workflowConfigurer).getWorkflowCustomActionNode(action));
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public WorkflowForkConfigurer<WorkflowForkConfigurer<T>> fork() {
    String forkNodeId = ((DefaultWorkflowConfigurer) workflowConfigurer).getNodeIdProvider().getUniqueNodeId();
    return new DefaultWorkflowForkConfigurer<WorkflowForkConfigurer<T>>
        (workflowConfigurer, (WorkflowForkConfigurer<WorkflowForkConfigurer<T>>) this, forkNodeId);
  }

  @Override
  public WorkflowForkConfigurer<T> also() {
    branches.add(new WorkflowForkBranch(currentBranch));
    currentBranch = Lists.newArrayList();
    return this;
  }

  public void addWorkflowForkNode(String forkNodeId, List<WorkflowForkBranch> branch) {
    currentBranch.add(new WorkflowForkNode(forkNodeId, branch));
  }

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public T join() {
    branches.add(new WorkflowForkBranch(currentBranch));
    if (parentForkConfigurer == null) {
      ((DefaultWorkflowConfigurer) workflowConfigurer).addWorkflowForkNode(forkNodeId, branches);
      return null;
    } else {
      ((DefaultWorkflowForkConfigurer<WorkflowForkConfigurer<T>>) parentForkConfigurer)
        .addWorkflowForkNode(forkNodeId, branches);
      return (T) parentForkConfigurer;
    }
  }
}
