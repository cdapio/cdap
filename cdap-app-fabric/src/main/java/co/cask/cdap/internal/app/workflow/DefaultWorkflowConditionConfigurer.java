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
import co.cask.cdap.api.customaction.CustomAction;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.workflow.WorkflowAction;
import co.cask.cdap.api.workflow.WorkflowConditionConfigurer;
import co.cask.cdap.api.workflow.WorkflowConditionNode;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowForkConfigurer;
import co.cask.cdap.api.workflow.WorkflowForkNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.proto.Id;
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
  private final String predicateClassName;
  private final String conditionNodeName;
  private final Id.Namespace deployNamespace;
  private final Id.Artifact artifactId;
  private final ArtifactRepository artifactRepository;
  private final PluginInstantiator pluginInstantiator;

  private List<WorkflowNode> currentBranch;
  private boolean addingToIfBranch = true;

  public DefaultWorkflowConditionConfigurer(String conditionNodeName, T parentConfigurer, String predicateClassName,
                                            Id.Namespace deployNamespace, Id.Artifact artifactId,
                                            ArtifactRepository artifactRepository,
                                            PluginInstantiator pluginInstantiator) {
    this.conditionNodeName = conditionNodeName;
    this.parentConfigurer = parentConfigurer;
    this.predicateClassName = predicateClassName;
    this.deployNamespace = deployNamespace;
    this.artifactId = artifactId;
    this.artifactRepository = artifactRepository;
    this.pluginInstantiator = pluginInstantiator;
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
  public WorkflowConditionConfigurer<T> addAction(CustomAction action) {
    currentBranch.add(WorkflowNodeCreator.createWorkflowCustomActionNode(action, deployNamespace, artifactId,
                                                                         artifactRepository, pluginInstantiator));
    return this;
  }

  @Override
  public WorkflowForkConfigurer<? extends WorkflowConditionConfigurer<T>> fork() {
    return new DefaultWorkflowForkConfigurer<>(this, deployNamespace, artifactId, artifactRepository,
                                               pluginInstantiator);
  }

  @Override
  @SuppressWarnings("unchecked")
  public WorkflowConditionConfigurer<? extends WorkflowConditionConfigurer<T>> condition(
    Predicate<WorkflowContext> predicate) {
    return new DefaultWorkflowConditionConfigurer<>(predicate.getClass().getSimpleName(), this,
                                                    predicate.getClass().getName(), deployNamespace, artifactId,
                                                    artifactRepository, pluginInstantiator);
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
    parentConfigurer.addWorkflowConditionNode(conditionNodeName, predicateClassName, ifBranch, elseBranch);
    return parentConfigurer;
  }

  @Override
  public void addWorkflowConditionNode(String conditionNodeName, String predicateClassName, List<WorkflowNode> ifBranch,
                                       List<WorkflowNode> elseBranch) {
    currentBranch.add(new WorkflowConditionNode(conditionNodeName, predicateClassName, ifBranch, elseBranch));
  }

  @Override
  public void addWorkflowForkNode(List<List<WorkflowNode>> branches) {
    currentBranch.add(new WorkflowForkNode(null, branches));
  }
}
