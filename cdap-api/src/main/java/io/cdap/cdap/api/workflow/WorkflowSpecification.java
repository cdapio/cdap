/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
package io.cdap.cdap.api.workflow;

import io.cdap.cdap.api.AbstractProgramSpecification;
import io.cdap.cdap.api.common.PropertyProvider;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.internal.dataset.DatasetCreationSpec;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Specification for a {@link Workflow}
 */
public final class WorkflowSpecification extends AbstractProgramSpecification implements
    PropertyProvider {

  private final Map<String, String> properties;
  private final List<WorkflowNode> nodes;
  private final Map<String, WorkflowNode> nodeIdMap;
  private final Map<String, DatasetCreationSpec> localDatasetSpecs;

  public WorkflowSpecification(String className, String name, String description,
      Map<String, String> properties, List<WorkflowNode> nodes,
      Map<String, DatasetCreationSpec> localDatasetSpecs, Map<String, Plugin> plugins) {
    super(className, name, description, plugins);
    this.properties = properties == null ? Collections.<String, String>emptyMap() :
        Collections.unmodifiableMap(new HashMap<>(properties));
    this.nodes = Collections.unmodifiableList(new ArrayList<>(nodes));
    this.nodeIdMap = Collections.unmodifiableMap(generateNodeIdMap(nodes));
    this.localDatasetSpecs = Collections.unmodifiableMap(new HashMap<>(localDatasetSpecs));
  }

  /**
   * Visit all the nodes in the {@link Workflow} and generate the map of node id to the {@link
   * WorkflowNode}.
   */
  private Map<String, WorkflowNode> generateNodeIdMap(List<WorkflowNode> nodesInWorkflow) {
    Map<String, WorkflowNode> nodeIdMap = new HashMap<>();
    Queue<WorkflowNode> nodes = new LinkedList<>(nodesInWorkflow);
    while (!nodes.isEmpty()) {
      WorkflowNode node = nodes.poll();
      nodeIdMap.put(node.getNodeId(), node);
      switch (node.getType()) {
        case ACTION:
          // do nothing. node already added to the nodeIdMap
          break;
        case FORK:
          WorkflowForkNode forkNode = (WorkflowForkNode) node;
          for (List<WorkflowNode> branch : forkNode.getBranches()) {
            nodes.addAll(branch);
          }
          break;
        case CONDITION:
          WorkflowConditionNode conditionNode = (WorkflowConditionNode) node;
          nodes.addAll(conditionNode.getIfBranch());
          nodes.addAll(conditionNode.getElseBranch());
          break;
        default:
          break;
      }
    }
    return nodeIdMap;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public String getProperty(String key) {
    return properties.get(key);
  }

  /**
   * Return the list of nodes in the {@link Workflow}.
   */
  public List<WorkflowNode> getNodes() {
    return nodes;
  }

  /**
   * Return the map of the node id to the {@link WorkflowNode}.
   */
  public Map<String, WorkflowNode> getNodeIdMap() {
    return nodeIdMap;
  }

  /**
   * Return the map of local dataset names and associated specifications required for dataset
   * instance creation.
   */
  public Map<String, DatasetCreationSpec> getLocalDatasetSpecs() {
    return localDatasetSpecs;
  }

  @Override
  public String toString() {
    return "WorkflowSpecification{"
        + "className='" + getClassName() + '\''
        + ", name='" + getName() + '\''
        + ", description='" + getDescription() + '\''
        + ", plugins=" + getPlugins()
        + ", properties=" + properties
        + ", nodes=" + nodes
        + ", nodeIdMap=" + nodeIdMap
        + ", localDatasetSpecs=" + localDatasetSpecs
        + '}';
  }
}
