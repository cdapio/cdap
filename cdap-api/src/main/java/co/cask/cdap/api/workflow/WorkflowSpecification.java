/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
package co.cask.cdap.api.workflow;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.common.PropertyProvider;

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
public final class WorkflowSpecification implements ProgramSpecification, PropertyProvider {
  private final String className;
  private final String name;
  private final String description;
  private final Map<String, String> properties;
  private final List<WorkflowNode> nodes;
  private final Map<String, WorkflowNode> nodeIdMap;

  public WorkflowSpecification(String className, String name, String description,
                               Map<String, String> properties, List<WorkflowNode> nodes) {
    this.className = className;
    this.name = name;
    this.description = description;
    this.properties = properties == null ? Collections.<String, String>emptyMap() :
                                           Collections.unmodifiableMap(new HashMap<>(properties));
    this.nodes = Collections.unmodifiableList(new ArrayList<>(nodes));
    this.nodeIdMap = Collections.unmodifiableMap(generateNodeIdMap(nodes));
  }
  /**
   +   * Visit all the nodes in the {@link Workflow} and generate the map of node id to
   +   * the {@link WorkflowNode}.
   +   */
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
  public String getClassName() {
    return className;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public String getProperty(String key) {
    return properties.get(key);
  }

  public List<WorkflowNode> getNodes() {
    return nodes;
  }

  public Map<String, WorkflowNode> getNodeIdMap() {
    return nodeIdMap;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("WorkflowSpecification{");
    sb.append("className='").append(className).append('\'');
    sb.append(", name='").append(name).append('\'');
    sb.append(", description='").append(description).append('\'');
    sb.append(", properties=").append(properties);
    sb.append(", nodes=").append(nodes);
    sb.append('}');
    return sb.toString();
  }
}
