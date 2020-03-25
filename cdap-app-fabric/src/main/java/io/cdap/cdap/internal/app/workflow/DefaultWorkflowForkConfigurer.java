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

package io.cdap.cdap.internal.app.workflow;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.cdap.cdap.api.Predicate;
import io.cdap.cdap.api.customaction.CustomAction;
import io.cdap.cdap.api.schedule.SchedulableProgramType;
import io.cdap.cdap.api.workflow.Condition;
import io.cdap.cdap.api.workflow.ConditionSpecification;
import io.cdap.cdap.api.workflow.WorkflowConditionConfigurer;
import io.cdap.cdap.api.workflow.WorkflowConditionNode;
import io.cdap.cdap.api.workflow.WorkflowContext;
import io.cdap.cdap.api.workflow.WorkflowForkConfigurer;
import io.cdap.cdap.api.workflow.WorkflowForkNode;
import io.cdap.cdap.api.workflow.WorkflowNode;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.internal.app.workflow.condition.DefaultConditionConfigurer;
import io.cdap.cdap.internal.workflow.condition.DefaultConditionSpecification;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Default implementation of the {@link WorkflowForkConfigurer}
 * @param <T>
 */
public class DefaultWorkflowForkConfigurer<T extends WorkflowForkJoiner & WorkflowConditionAdder>
  implements WorkflowForkConfigurer<T>, WorkflowForkJoiner, WorkflowConditionAdder {

  private final T parentForkConfigurer;
  private final List<List<WorkflowNode>> branches = Lists.newArrayList();
  private final Id.Namespace deployNamespace;
  private final Id.Artifact artifactId;
  private final PluginFinder pluginFinder;
  private final PluginInstantiator pluginInstantiator;

  private List<WorkflowNode> currentBranch;

  public DefaultWorkflowForkConfigurer(T parentForkConfigurer, Id.Namespace deployNamespace, Id.Artifact artifactId,
                                       PluginFinder pluginFinder, PluginInstantiator pluginInstantiator) {
    this.parentForkConfigurer = parentForkConfigurer;
    currentBranch = Lists.newArrayList();
    this.deployNamespace = deployNamespace;
    this.artifactId = artifactId;
    this.pluginFinder = pluginFinder;
    this.pluginInstantiator = pluginInstantiator;
  }

  @Override
  public WorkflowForkConfigurer<T> addMapReduce(String mapReduce) {
    currentBranch.add(WorkflowNodeCreator.createWorkflowActionNode(mapReduce, SchedulableProgramType.MAPREDUCE));
    return this;
  }

  @Override
  public WorkflowForkConfigurer<T> addSpark(String spark) {
    currentBranch.add(WorkflowNodeCreator.createWorkflowActionNode(spark, SchedulableProgramType.SPARK));
    return this;
  }

  @Override
  public WorkflowForkConfigurer<T> addAction(CustomAction action) {
    currentBranch.add(WorkflowNodeCreator.createWorkflowCustomActionNode(action, deployNamespace, artifactId,
                                                                         pluginFinder, pluginInstantiator));
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public WorkflowForkConfigurer<? extends WorkflowForkConfigurer<T>> fork() {
    return new DefaultWorkflowForkConfigurer<>(this, deployNamespace, artifactId, pluginFinder,
                                               pluginInstantiator);
  }

  @Override
  public WorkflowConditionConfigurer<? extends WorkflowForkConfigurer<T>> condition(
    Predicate<WorkflowContext> predicate) {
    return new DefaultWorkflowConditionConfigurer<>(predicate, this, deployNamespace, artifactId, pluginFinder,
                                                    pluginInstantiator);
  }

  @Override
  public WorkflowConditionConfigurer<? extends WorkflowForkConfigurer<T>> condition(Condition condition) {
    return new DefaultWorkflowConditionConfigurer<>(condition, this, deployNamespace, artifactId, pluginFinder,
                                                    pluginInstantiator);
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

  @Override
  public void addWorkflowConditionNode(Predicate<WorkflowContext> predicate, List<WorkflowNode> ifBranch,
                                       List<WorkflowNode> elseBranch) {
    ConditionSpecification spec = new DefaultConditionSpecification(predicate.getClass().getName(),
                                                                    predicate.getClass().getSimpleName(), "",
                                                                    new HashMap<String, String>(),
                                                                    new HashSet<String>());
    currentBranch.add(new WorkflowConditionNode(spec.getName(), spec, ifBranch, elseBranch));
  }

  @Override
  public void addWorkflowConditionNode(Condition condition, List<WorkflowNode> ifBranch,
                                       List<WorkflowNode> elseBranch) {
    Preconditions.checkArgument(condition != null, "Condition is null.");
    ConditionSpecification spec = DefaultConditionConfigurer.configureCondition(condition, deployNamespace,
                                                                                artifactId, pluginFinder,
                                                                                pluginInstantiator);
    currentBranch.add(new WorkflowConditionNode(spec.getName(), spec, ifBranch, elseBranch));
  }
}
