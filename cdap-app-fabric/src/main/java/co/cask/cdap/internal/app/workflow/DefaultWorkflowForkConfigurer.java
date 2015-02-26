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
import co.cask.cdap.api.workflow.WorkflowForkNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import com.clearspring.analytics.util.Lists;

import java.util.List;

/**
 * Default implementation of the {@link WorkflowForkConfigurer}
 * @param <T>
 */
public class DefaultWorkflowForkConfigurer<T extends WorkflowForkJoiner>
  implements WorkflowForkConfigurer<T>, WorkflowForkJoiner {

  private final T parentForkConfigurer;

  private final List<List<WorkflowNode>> branches = Lists.newArrayList();
  private List<WorkflowNode> currentBranch;

  public DefaultWorkflowForkConfigurer(T parentForkConfigurer) {
    this.parentForkConfigurer = parentForkConfigurer;
    currentBranch = Lists.newArrayList();
  }

  @Override
  public WorkflowForkConfigurer<T> addMapReduce(String mapReduce) {
    currentBranch.add(WorkflowNodeCreator.createWorkflowActionNode(mapReduce, SchedulableProgramType.MAPREDUCE));
    return this;
  }

  @Override
  public WorkflowForkConfigurer<T> addSpark(String spark) {
    currentBranch.add(WorkflowNodeCreator.createWorkflowActionNode(spark, SchedulableProgramType.CUSTOM_ACTION));
    return this;
  }

  @Override
  public WorkflowForkConfigurer<T> addAction(WorkflowAction action) {
    currentBranch.add(WorkflowNodeCreator.createWorkflowCustomActionNode(action));
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public WorkflowForkConfigurer<? extends WorkflowForkConfigurer<T>> fork() {
    return new DefaultWorkflowForkConfigurer<DefaultWorkflowForkConfigurer<T>>(this);
  }

  @Override
  public WorkflowForkConfigurer<T> also() {
    branches.add(currentBranch);
    currentBranch = Lists.newArrayList();
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T join() {
    branches.add(currentBranch);
    parentForkConfigurer.addWorkflowForkNode(branches);
    return parentForkConfigurer;
  }

  @Override
  public void addWorkflowForkNode(List<List<WorkflowNode>> branches) {
    currentBranch.add(new WorkflowForkNode(null, branches));
  }
}
