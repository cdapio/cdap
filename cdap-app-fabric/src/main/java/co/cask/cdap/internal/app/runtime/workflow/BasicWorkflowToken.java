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
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of the {@link WorkflowToken} interface.
 */
public class BasicWorkflowToken implements WorkflowToken {
  private Map<String, Map<String, Long>> mapReduceCounters;
  private final Table<Scope, String, List<NodeValueEntry>> tokenValueMap = HashBasedTable.create();
  private String nodeName;

  public BasicWorkflowToken() {
  }

  public BasicWorkflowToken(Table<Scope, String, List<NodeValueEntry>> tokenValueMap, String nodeName,
                            @Nullable Map<String, Map<String, Long>> mapReduceCounters) {
    for (Table.Cell<Scope, String, List<NodeValueEntry>> cell : tokenValueMap.cellSet()) {
      List<NodeValueEntry> nodeValueList = Lists.newArrayList();
      nodeValueList.addAll(cell.getValue());
      this.tokenValueMap.put(cell.getRowKey(), cell.getColumnKey(), nodeValueList);
    }

    this.nodeName = nodeName;

    if (mapReduceCounters != null) {
      this.mapReduceCounters = copyHadoopCounters(mapReduceCounters);
    }
  }

  public BasicWorkflowToken(BasicWorkflowToken other) {
    this(other.tokenValueMap, other.nodeName, other.mapReduceCounters);
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
    for (Table.Cell<Scope, String, List<NodeValueEntry>> cell : other.tokenValueMap.cellSet()) {
      if (!tokenValueMap.contains(cell.getRowKey(), cell.getColumnKey())) {
        // Key is newly added to the other WorkflowToken
        tokenValueMap.put(cell.getRowKey(), cell.getColumnKey(), Lists.<NodeValueEntry>newArrayList());
      }
      // Iterate over the list of NodeValueEntry corresponding to the current key.
      // Only add those NodeValueEntry to the merged token which already do not exist.

      for (NodeValueEntry otherNodeValueEntry : cell.getValue()) {
        boolean otherNodeValueEntryExist = false;
        for (NodeValueEntry thisNodeValueEntry : tokenValueMap.get(cell.getRowKey(), cell.getColumnKey())) {
          if (thisNodeValueEntry.equals(otherNodeValueEntry)) {
            otherNodeValueEntryExist = true;
            break;
          }
        }
        if (!otherNodeValueEntryExist) {
          tokenValueMap.get(cell.getRowKey(), cell.getColumnKey()).add(otherNodeValueEntry);
        }
      }
    }

    if (other.getMapReduceCounters() != null) {
      setMapReduceCounters(other.getMapReduceCounters());
    }
  }

  @Override
  public void put(String key, String value) {
    put(key, value, Scope.USER);
  }

  @Override
  public void put(String key, Value value) {
    put(key, value.toString());
  }

  void put(String key, String value, Scope scope) {
    if (nodeName == null) {
      throw new IllegalStateException("Node name cannot be null.");
    }

    List<NodeValueEntry> nodeValueList = tokenValueMap.get(scope, key);
    if (nodeValueList == null) {
      nodeValueList = Lists.newArrayList();
      tokenValueMap.put(scope, key, nodeValueList);
    }

    // Check if the current node already added the key to the token.
    // In that case replace that entry with the new one
    for (int i = 0; i < nodeValueList.size(); i++) {
      if (nodeValueList.get(i).getNodeName().equals(nodeName)) {
        nodeValueList.set(i, new NodeValueEntry(nodeName, new Value(value)));
        return;
      }
    }

    nodeValueList.add(new NodeValueEntry(nodeName, new Value(value)));
  }

  @Nullable
  @Override
  public Value get(String key) {
    return get(key, Scope.USER);
  }

  @Nullable
  @Override
  public Value get(String key, Scope scope) {
    List<NodeValueEntry> nodeValueList = tokenValueMap.get(scope, key);
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

  @Nullable
  @Override
  public Value get(String key, String nodeName) {
    return get(key, nodeName, Scope.USER);
  }

  @Nullable
  @Override
  public Value get(String key, String nodeName, Scope scope) {
    List<NodeValueEntry> nodeValueList = tokenValueMap.get(scope, key);
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
    if (containsKey(key, Scope.USER)) {
      return ImmutableList.copyOf(tokenValueMap.get(scope, key));
    }
    return ImmutableList.of();
  }

  @Override
  public Map<String, Value> getAllFromNode(String nodeName) {
    return getAllFromNode(nodeName, Scope.USER);
  }

  @Override
  public Map<String, Value> getAllFromNode(String nodeName, Scope scope) {
    Map<String, Value> tokenValuesFromNode = Maps.newHashMap();
    for (Map.Entry<String, List<NodeValueEntry>> entry : tokenValueMap.rowMap().get(scope).entrySet()) {

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
  public Map<String, List<NodeValueEntry>> getAll(Scope scope) {
    return ImmutableMap.copyOf(tokenValueMap.rowMap().get(scope));
  }

  @Deprecated
  @Nullable
  @Override
  public Map<String, Map<String, Long>> getMapReduceCounters() {
    return mapReduceCounters;
  }

  @Override
  public boolean containsKey(String key, Scope scope) {
    return tokenValueMap.contains(scope, key);
  }

  @Override
  public boolean containsKey(String key) {
    return containsKey(key, Scope.USER);
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
