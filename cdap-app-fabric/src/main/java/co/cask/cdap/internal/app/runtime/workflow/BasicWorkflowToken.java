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
import co.cask.cdap.api.workflow.WorkflowToken;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of the {@link WorkflowToken} interface.
 */
public class BasicWorkflowToken implements WorkflowToken {
  private Map<String, Map<String, Long>> mapReduceCounters;
  private final Map<String, List<NodeValueEntry>> tokenValueMap = Maps.newHashMap();
  private String nodeName;

  void setCurrentNode(String nodeName) {
    this.nodeName = nodeName;
  }

  /**
   * Merge the other WorkflowToken passed to the method as a parameter
   * with the WorkflowToken on which the method is invoked.
   * @param other the other WorkflowToken to be merged
   */
  void mergeToken(BasicWorkflowToken other) {
    Map<String, List<NodeValueEntry>> otherTokenValueMap = other.tokenValueMap;

    for (Map.Entry<String, List<NodeValueEntry>> entry : otherTokenValueMap.entrySet()) {
      if (!tokenValueMap.containsKey(entry.getKey())) {
        // Key is newly added to the other WorkflowToken
        tokenValueMap.put(entry.getKey(), Lists.<NodeValueEntry>newArrayList());
      }

      // Iterate over the list of NodeValueEntry corresponding to the current key.
      // Only add those NodeValueEntry to the merged token which already do not exist.
      for (NodeValueEntry otherNodeValueEntry : otherTokenValueMap.get(entry.getKey())) {
        boolean otherNodeValueEntryExist = false;
        for (NodeValueEntry thisNodeValueEntry : tokenValueMap.get(entry.getKey())) {
          if (thisNodeValueEntry.equals(otherNodeValueEntry)) {
            otherNodeValueEntryExist = true;
            break;
          }
        }
        if (!otherNodeValueEntryExist) {
          tokenValueMap.get(entry.getKey()).add(otherNodeValueEntry);
        }
      }
    }
    if (other.getMapReduceCounters() != null) {
      setMapReduceCounters(other.getMapReduceCounters());
    }
  }

  @Override
  public void put(String key, String value) {
    if (nodeName == null) {
      throw new IllegalStateException("Node name cannot be null.");
    }
    List<NodeValueEntry> nodeValueList = tokenValueMap.get(key);
    if (nodeValueList == null) {
      nodeValueList = Lists.newArrayList();
      tokenValueMap.put(key, nodeValueList);
    }
    nodeValueList.add(new NodeValueEntry(nodeName, value));
  }

  @Override
  public List<NodeValueEntry> getAll(String key) {
    if (containsKey(key)) {
      return ImmutableList.copyOf(tokenValueMap.get(key));
    }
    return ImmutableList.of();
  }

  @Override
  public Map<String, String> getAllFromNode(String nodeName, @Nullable String prefix) {
    Map<String, String> tokenValuesFromNode = Maps.newHashMap();
    for (Map.Entry<String, List<NodeValueEntry>> entry : tokenValueMap.entrySet()) {

      if (prefix != null) {
        if (!entry.getKey().startsWith(prefix)) {
          continue;
        }
      }

      List<NodeValueEntry> nodeValueEntryList = entry.getValue();
      for (NodeValueEntry nodeValueEntry : nodeValueEntryList) {
        if (nodeValueEntry.getNodeName().equals(nodeName)) {
          tokenValuesFromNode.put(entry.getKey(), nodeValueEntry.getValue());
        }
      }
    }
    return tokenValuesFromNode;
  }

  @Nullable
  @Override
  public String get(String key, String nodeName) {
    if (!containsKey(key)) {
      return null;
    }

    List<NodeValueEntry> nodeValueList = tokenValueMap.get(key);
    for (NodeValueEntry nodeValue : nodeValueList) {
      if (nodeValue.getNodeName().equals(nodeName)) {
        return nodeValue.getValue();
      }
    }
    return null;
  }

  @Nullable
  @Override
  public String get(String key) {
    if (!containsKey(key)) {
      return null;
    }
    List<NodeValueEntry> nodeValueList = tokenValueMap.get(key);
    if (nodeValueList.isEmpty()) {
      // List of NodeValueEntry cannot be empty if the key is added in the WorkflowToken as
      // when we add key, we also add single NodeValueEntry.
      throw new IllegalStateException(String.format("List of NodeValueEntry for the key %s cannot be empty", key));
    }
    return nodeValueList.get(nodeValueList.size() - 1).getValue();
  }

  @Deprecated
  @Nullable
  @Override
  public Map<String, Map<String, Long>> getMapReduceCounters() {
    return mapReduceCounters;
  }

  @Override
  public boolean containsKey(String key) {
    return tokenValueMap.containsKey(key);
  }

  public void setMapReduceCounters(Map<String, Map<String, Long>> mapReduceCounters) {
    this.mapReduceCounters = copyHadoopCounters(mapReduceCounters);
  }

  /**
   * Make a deep copy of the {@link WorkflowToken}.
   * @return copied WorkflowToken
   */
  public WorkflowToken deepCopy() {
    BasicWorkflowToken copiedToken = new BasicWorkflowToken();
    if (getMapReduceCounters() != null) {
      copiedToken.setMapReduceCounters(copyHadoopCounters(getMapReduceCounters()));
    }

    copiedToken.setTokenValueMap(tokenValueMap);
    copiedToken.setCurrentNode(nodeName);
    return copiedToken;
  }

  private Map<String, Map<String, Long>> copyHadoopCounters(Map<String, Map<String, Long>> input) {
    ImmutableMap.Builder<String, Map<String, Long>> builder = ImmutableMap.builder();
    for (Map.Entry<String, Map<String, Long>> entry : input.entrySet()) {
      builder.put(entry.getKey(), ImmutableMap.copyOf(entry.getValue()));
    }
    return builder.build();
  }

  private void setTokenValueMap(Map<String, List<NodeValueEntry>> otherTokenValueMap) {
    tokenValueMap.clear();
    for (Map.Entry<String, List<NodeValueEntry>> entry : otherTokenValueMap.entrySet()) {
      List<NodeValueEntry> nodeValueList = Lists.newArrayList();
      nodeValueList.addAll(entry.getValue());
      tokenValueMap.put(entry.getKey(), nodeValueList);
    }
  }
}
