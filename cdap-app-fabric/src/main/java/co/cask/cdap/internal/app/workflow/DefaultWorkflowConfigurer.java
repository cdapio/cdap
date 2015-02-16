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
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowAction;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.api.workflow.WorkflowConfigurer;
import co.cask.cdap.api.workflow.WorkflowFork;
import co.cask.cdap.api.workflow.WorkflowForkBranch;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowNodeType;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.internal.workflow.DefaultWorkflowActionSpecification;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of {@link WorkflowConfigurer}.
 */
public class DefaultWorkflowConfigurer implements WorkflowConfigurer {

  private final String className;
  private String name;
  private String description;
  private Map<String, String> properties;

  private final List<WorkflowNode> nodes = Lists.newArrayList();
  private final Map<String, WorkflowFork> forks = Maps.newHashMap();
  private final Map<String, List<WorkflowNode>> branches = Maps.newHashMap();
  private final Set<String> mapreduces = Sets.newHashSet();
  private final Set<String> sparks = Sets.newHashSet();
  private final Map<String, WorkflowActionSpecification> customActionMap = Maps.newHashMap();

  public DefaultWorkflowConfigurer(Workflow workflow) {
    this.className = workflow.getClass().getName();
    this.name = workflow.getClass().getSimpleName();
    this.description = "";
  }

  @Override
  public void setName(String name) {
    this.name = name;
  }

  @Override
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public void setProperties(Map<String, String> properties) {
    this.properties = ImmutableMap.copyOf(properties);
  }

  @Override
  public void addMapReduce(String mapReduce) {
    Preconditions.checkNotNull(mapReduce, "MapReduce name is null.");
    Preconditions.checkArgument(!mapReduce.isEmpty(), "MapReduce name is empty.");
    mapreduces.add(mapReduce);
    nodes.add(new WorkflowNode(mapReduce, WorkflowNodeType.ACTION));
  }

  @Override
  public void addSpark(String spark) {
    Preconditions.checkNotNull(spark, "Spark program name is null.");
    Preconditions.checkArgument(!spark.isEmpty(), "Spark program name is empty.");
    sparks.add(spark);
    nodes.add(new WorkflowNode(spark, WorkflowNodeType.ACTION));
  }

  @Override
  public void addAction(WorkflowAction action) {
    Preconditions.checkArgument(action != null, "WorkflowAction is null.");
    WorkflowActionSpecification spec = new DefaultWorkflowActionSpecification(action);
    customActionMap.put(spec.getName(), spec);
    nodes.add(new WorkflowNode(spec.getName(), WorkflowNodeType.ACTION));
  }

  @Override
  public void addMapReduceToBranch(String mapReduce, String branch) {
    Preconditions.checkNotNull(mapReduce, "MapReduce name is null.");
    Preconditions.checkArgument(!mapReduce.isEmpty(), "MapReduce name is empty.");
    Preconditions.checkNotNull(branch, "Branch name is null.");
    Preconditions.checkArgument(!branch.isEmpty(), "Branch name is empty.");

    mapreduces.add(mapReduce);
    if (!branches.containsKey(branch)) {
      branches.put(branch, new ArrayList<WorkflowNode>());
    }
    WorkflowNode node = new WorkflowNode(mapReduce, WorkflowNodeType.ACTION);
    branches.get(branch).add(node);
  }

  @Override
  public void addSparkToBranch(String spark, String branch) {
    Preconditions.checkNotNull(spark, "Spark program name is null.");
    Preconditions.checkArgument(!spark.isEmpty(), "Spark program name is empty.");
    Preconditions.checkNotNull(branch, "Branch name is null.");
    Preconditions.checkArgument(!branch.isEmpty(), "Branch name is empty.");

    sparks.add(spark);
    if (!branches.containsKey(branch)) {
      branches.put(branch, new ArrayList<WorkflowNode>());
    }
    WorkflowNode node = new WorkflowNode(spark, WorkflowNodeType.ACTION);
    branches.get(branch).add(node);
  }

  @Override
  public void addActionToBranch(WorkflowAction action, String branch) {
    Preconditions.checkArgument(action != null, "WorkflowAction is null.");
    Preconditions.checkNotNull(branch, "Branch name is null.");
    Preconditions.checkArgument(!branch.isEmpty(), "Branch name is empty.");

    WorkflowActionSpecification spec = new DefaultWorkflowActionSpecification(action);
    customActionMap.put(spec.getName(), spec);

    if (!branches.containsKey(branch)) {
      branches.put(branch, new ArrayList<WorkflowNode>());
    }
    WorkflowNode node = new WorkflowNode(spec.getName(), WorkflowNodeType.ACTION);
    branches.get(branch).add(node);
  }

  @Override
  public void addFork(String fork, List<String> branchList) {
    Preconditions.checkArgument(branchList != null, "List of branches for the fork is null.");
    Preconditions.checkArgument(branchList.size() > 1, "Atleast two branches needed for fork.");

    List<WorkflowForkBranch> forkBranches = Lists.newArrayList();

    for (String branch : branchList) {
      Preconditions.checkArgument(branches.containsKey(branch), "Fork branch '" + branch + "' is not found.");
      WorkflowForkBranch forkBranch = new WorkflowForkBranch(branch, branches.get(branch));
      forkBranches.add(forkBranch);
    }

    forks.put(fork, new WorkflowFork(forkBranches));

    WorkflowNode node = new WorkflowNode(fork, WorkflowNodeType.FORK);
    nodes.add(node);
  }

  public WorkflowSpecification createSpecification() {
    return new WorkflowSpecification(className, name, description, properties, nodes, forks, mapreduces, sparks,
                                     customActionMap);
  }
}
