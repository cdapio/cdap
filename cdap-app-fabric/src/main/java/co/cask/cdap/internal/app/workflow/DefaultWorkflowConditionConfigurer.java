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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.workflow.WorkflowAction;
import co.cask.cdap.api.workflow.WorkflowConditionConfigurer;
import co.cask.cdap.api.workflow.WorkflowConditionNode;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowForkConfigurer;
import co.cask.cdap.api.workflow.WorkflowForkNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Default implementation of the {@link WorkflowConditionConfigurer}.
 * @param <T> the type of the parent configurer
 */
public class DefaultWorkflowConditionConfigurer<T extends WorkflowConditionAdder & WorkflowForkJoiner>
  implements WorkflowConditionConfigurer<T>, WorkflowConditionAdder, WorkflowForkJoiner {

  private final T parentConfigurer;
  private final List<WorkflowNode> ifBranch = Lists.newArrayList();
  private final List<WorkflowNode> elseBranch = Lists.newArrayList();
  private List<WorkflowNode> currentBranch;
  private boolean addingToIfBranch = true;
  private final String predicateClassName;

  public DefaultWorkflowConditionConfigurer(T parentConfigurer, String predicateClassName) {
    this.parentConfigurer = parentConfigurer;
    this.predicateClassName = predicateClassName;
    currentBranch = Lists.newArrayList();
  }

  @Override
  public WorkflowConditionConfigurer<T> addMapReduce(String mapReduce) {
    currentBranch.add(WorkflowNodeCreator.createWorkflowActionNode(mapReduce, SchedulableProgramType.MAPREDUCE));
    return this;
  }

  @Override
  public WorkflowConditionConfigurer<T> addSpark(String spark) {
    currentBranch.add(WorkflowNodeCreator.createWorkflowActionNode(spark, SchedulableProgramType.SPARK));
    return this;
  }

  @Override
  public WorkflowConditionConfigurer<T> addAction(WorkflowAction action) {
    currentBranch.add(WorkflowNodeCreator.createWorkflowCustomActionNode(action));
    return this;
  }

  @Override
  public WorkflowForkConfigurer<? extends WorkflowConditionConfigurer<T>> fork() {
    return new DefaultWorkflowForkConfigurer<DefaultWorkflowConditionConfigurer<T>>(this);
  }

  @Override
  @SuppressWarnings("unchecked")
  public WorkflowConditionConfigurer<? extends WorkflowConditionConfigurer<T>> condition(
    Predicate<WorkflowContext> predicate) {
    String predicateClassName = predicate.getClass().getName();
    return new DefaultWorkflowConditionConfigurer<DefaultWorkflowConditionConfigurer<T>>(this, predicateClassName);
  }

  @Override
  public WorkflowConditionConfigurer<T> otherwise() {
    ifBranch.addAll(currentBranch);
    addingToIfBranch = false;
    currentBranch = Lists.newArrayList();
    return this;
  }

  @Override
  public T end() {
    if (addingToIfBranch) {
      ifBranch.addAll(currentBranch);
    } else {
      elseBranch.addAll(currentBranch);
    }
    parentConfigurer.addWorkflowConditionNode(predicateClassName, ifBranch, elseBranch);
    return parentConfigurer;
  }

  @Override
  public void addWorkflowConditionNode(String predicateClassName, List<WorkflowNode> ifBranch,
                                       List<WorkflowNode> elseBranch) {
    currentBranch.add(new WorkflowConditionNode(null, predicateClassName, ifBranch, elseBranch));
  }

  @Override
  public void addWorkflowForkNode(List<List<WorkflowNode>> branches) {
    currentBranch.add(new WorkflowForkNode(null, branches));
  }
}
