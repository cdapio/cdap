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

package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.workflow.NodeValueEntry;
import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowToken;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the {@link WorkflowToken} interface.
 */
public class BasicWorkflowToken implements WorkflowToken {
  private Map<String, Map<String, Long>> mapReduceCounters;
  private final Map<Scope, Map<String, List<NodeValueEntry>>> tokenValueMap = new EnumMap<>(Scope.class);
  private String nodeName;

  public BasicWorkflowToken() {
    for (Scope scope : Scope.values()) {
      tokenValueMap.put(scope, new HashMap<String, List<NodeValueEntry>>());
    }
  }

  private BasicWorkflowToken(BasicWorkflowToken other) {
    for (Map.Entry<Scope, Map<String, List<NodeValueEntry>>> entry : other.tokenValueMap.entrySet()) {
      Map<String, List<NodeValueEntry>> tokenValueMapForScope = new HashMap<>();
      for (Map.Entry<String, List<NodeValueEntry>> valueEntry : entry.getValue().entrySet()) {
        tokenValueMapForScope.put(valueEntry.getKey(), Lists.newArrayList(valueEntry.getValue()));
      }

      this.tokenValueMap.put(entry.getKey(), tokenValueMapForScope);
    }

    this.nodeName = other.nodeName;

    if (other.mapReduceCounters != null) {
      this.mapReduceCounters = copyHadoopCounters(other.mapReduceCounters);
    }
  }

  void setCurrentNode(String nodeName) {
    this.nodeName = nodeName;
  }

  /**
   * Merge the other WorkflowToken passed to the method as a parameter
   * with the WorkflowToken on which the method is invoked.
   * @param other the other WorkflowToken to be merged
   */
  void mergeToken(BasicWorkflowToken other) {
    for (Map.Entry<Scope, Map<String, List<NodeValueEntry>>> entry : other.tokenValueMap.entrySet()) {
      Map<String, List<NodeValueEntry>> thisTokenValueMapForScope = this.tokenValueMap.get(entry.getKey());

      for (Map.Entry<String, List<NodeValueEntry>> otherTokenValueMapForScopeEntry : entry.getValue().entrySet()) {
        if (!thisTokenValueMapForScope.containsKey(otherTokenValueMapForScopeEntry.getKey())) {
          thisTokenValueMapForScope.put(otherTokenValueMapForScopeEntry.getKey(), Lists.<NodeValueEntry>newArrayList());
        }

        // Iterate over the list of NodeValueEntry corresponding to the current key.
        // Only add those NodeValueEntry to the merged token which already do not exist.

        for (NodeValueEntry otherNodeValueEntry : otherTokenValueMapForScopeEntry.getValue()) {
          boolean otherNodeValueEntryExist = false;
          for (NodeValueEntry thisNodeValueEntry :
            thisTokenValueMapForScope.get(otherTokenValueMapForScopeEntry.getKey())) {
            if (thisNodeValueEntry.equals(otherNodeValueEntry)) {
              otherNodeValueEntryExist = true;
              break;
            }
          }
          if (!otherNodeValueEntryExist) {
            thisTokenValueMapForScope.get(otherTokenValueMapForScopeEntry.getKey()).add(otherNodeValueEntry);
          }
        }
      }
    }

    if (other.getMapReduceCounters() != null) {
      setMapReduceCounters(other.getMapReduceCounters());
    }
  }

  @Override
  public void put(String key, String value) {
    put(key, Value.of(value));
  }

  @Override
  public void put(String key, Value value) {
    put(key, value, Scope.USER);
  }

  void put(String key, Value value, Scope scope) {
    Preconditions.checkNotNull(key, "Null key cannot be added in the WorkflowToken.");
    Preconditions.checkNotNull(value, String.format("Null value provided for the key '%s'.", key));
    Preconditions.checkNotNull(value.toString(), String.format("Null value provided for the key '%s'.", key));
    Preconditions.checkState(nodeName != null, "nodeName cannot be null.");

    List<NodeValueEntry> nodeValueList = tokenValueMap.get(scope).get(key);
    if (nodeValueList == null) {
      nodeValueList = Lists.newArrayList();
      tokenValueMap.get(scope).put(key, nodeValueList);
    }

    // Check if the current node already added the key to the token.
    // In that case replace that entry with the new one
    for (int i = 0; i < nodeValueList.size(); i++) {
      if (nodeValueList.get(i).getNodeName().equals(nodeName)) {
        nodeValueList.set(i, new NodeValueEntry(nodeName, value));
        return;
      }
    }

    nodeValueList.add(new NodeValueEntry(nodeName, value));
  }

  @Override
  public Value get(String key) {
    return get(key, Scope.USER);
  }

  @Override
  public Value get(String key, Scope scope) {
    List<NodeValueEntry> nodeValueList = tokenValueMap.get(scope).get(key);
    if (nodeValueList == null) {
      return null;
    }

    if (nodeValueList.isEmpty()) {
      // List of NodeValueEntry cannot be empty if the key is added in the WorkflowToken as
      // when we add key, we also add single NodeValueEntry.
      throw new IllegalStateException(String.format("List of NodeValueEntry for the key %s cannot be empty", key));
    }
    return nodeValueList.get(nodeValueList.size() - 1).getValue();
  }

  @Override
  public Value get(String key, String nodeName) {
    return get(key, nodeName, Scope.USER);
  }

  @Override
  public Value get(String key, String nodeName, Scope scope) {
    List<NodeValueEntry> nodeValueList = tokenValueMap.get(scope).get(key);
    if (nodeValueList == null) {
      return null;
    }

    for (NodeValueEntry nodeValue : nodeValueList) {
      if (nodeValue.getNodeName().equals(nodeName)) {
        return nodeValue.getValue();
      }
    }
    return null;
  }

  @Override
  public List<NodeValueEntry> getAll(String key) {
    return getAll(key, Scope.USER);
  }

  @Override
  public List<NodeValueEntry> getAll(String key, Scope scope) {
    if (tokenValueMap.get(scope).containsKey(key)) {
      return ImmutableList.copyOf(tokenValueMap.get(scope).get(key));
    }
    return ImmutableList.of();
  }

  @Override
  public Map<String, Value> getAllFromNode(String nodeName) {
    return getAllFromNode(nodeName, Scope.USER);
  }

  @Override
  public Map<String, Value> getAllFromNode(String nodeName, Scope scope) {
    Map<String, Value> tokenValuesFromNode = new HashMap<>();
    for (Map.Entry<String, List<NodeValueEntry>> entry : tokenValueMap.get(scope).entrySet()) {

      List<NodeValueEntry> nodeValueEntryList = entry.getValue();
      for (NodeValueEntry nodeValueEntry : nodeValueEntryList) {
        if (nodeValueEntry.getNodeName().equals(nodeName)) {
          tokenValuesFromNode.put(entry.getKey(), nodeValueEntry.getValue());
          break;
        }
      }
    }
    return tokenValuesFromNode;
  }

  @Override
  public Map<String, List<NodeValueEntry>> getAll() {
    return getAll(Scope.USER);
  }

  @Override
  public Map<String, List<NodeValueEntry>> getAll(Scope scope) {
    return ImmutableMap.copyOf(tokenValueMap.get(scope));
  }

  @Override
  public Map<String, Map<String, Long>> getMapReduceCounters() {
    return mapReduceCounters;
  }

  public void setMapReduceCounters(Map<String, Map<String, Long>> mapReduceCounters) {
    this.mapReduceCounters = copyHadoopCounters(mapReduceCounters);
  }

  /**
   * Make a deep copy of the {@link WorkflowToken}.
   * @return copied WorkflowToken
   */
  public WorkflowToken deepCopy() {
    return new BasicWorkflowToken(this);
  }

  private Map<String, Map<String, Long>> copyHadoopCounters(Map<String, Map<String, Long>> input) {
    ImmutableMap.Builder<String, Map<String, Long>> builder = ImmutableMap.builder();
    for (Map.Entry<String, Map<String, Long>> entry : input.entrySet()) {
      builder.put(entry.getKey(), ImmutableMap.copyOf(entry.getValue()));
    }
    return builder.build();
  }
}
