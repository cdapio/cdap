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

import co.cask.cdap.api.workflow.NodeValue;
import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowToken;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of the {@link WorkflowToken} interface.
 */
public class BasicWorkflowToken implements WorkflowToken, Serializable {

  private static final long serialVersionUID = -1173500180640174909L;
  private static final Object sharedLock = new Object();

  private Map<String, Map<String, Long>> mapReduceCounters;
  private final Map<Scope, Map<String, List<NodeValue>>> tokenValueMap = new EnumMap<>(Scope.class);
  private String nodeName;
  private Set<String> nodeFilterSet;

  public BasicWorkflowToken() {
    for (Scope scope : Scope.values()) {
      tokenValueMap.put(scope, new ConcurrentHashMap<String, List<NodeValue>>());
    }
  }

  private BasicWorkflowToken(BasicWorkflowToken other) {
    // Same instance of the tokenValueMap is shared across all the
    // nodes in the Workflow, so that values can be put concurrently.
    for (Scope scope : Scope.values()) {
      tokenValueMap.put(scope, other.tokenValueMap.get(scope));
    }

    if (other.mapReduceCounters != null) {
      this.mapReduceCounters = copyHadoopCounters(other.mapReduceCounters);
    }
  }

  void setCurrentNodeInfo(String nodeName, Set<String> nodeFilterList) {
    this.nodeName = nodeName;
    this.nodeFilterSet = nodeFilterList;
  }

  /**
   * Merge the other WorkflowToken passed to the method as a parameter
   * with the WorkflowToken on which the method is invoked.
   * @param other the other WorkflowToken to be merged
   */
  void mergeToken(BasicWorkflowToken other) {
    synchronized (sharedLock) {
      for (Map.Entry<Scope, Map<String, List<NodeValue>>> entry : other.tokenValueMap.entrySet()) {
        Map<String, List<NodeValue>> thisTokenValueMapForScope = this.tokenValueMap.get(entry.getKey());

        for (Map.Entry<String, List<NodeValue>> otherTokenValueMapForScopeEntry : entry.getValue().entrySet()) {
          if (!thisTokenValueMapForScope.containsKey(otherTokenValueMapForScopeEntry.getKey())) {
            thisTokenValueMapForScope.put(otherTokenValueMapForScopeEntry.getKey(), Lists.<NodeValue>newArrayList());
          }

          // Iterate over the list of NodeValue corresponding to the current key.
          // Only add those NodeValue to the merged token which already do not exist.

          for (NodeValue otherNodeValue : otherTokenValueMapForScopeEntry.getValue()) {
            boolean otherNodeValueExist = false;
            for (NodeValue thisNodeValue :
              thisTokenValueMapForScope.get(otherTokenValueMapForScopeEntry.getKey())) {
              if (thisNodeValue.equals(otherNodeValue)) {
                otherNodeValueExist = true;
                break;
              }
            }
            if (!otherNodeValueExist) {
              thisTokenValueMapForScope.get(otherTokenValueMapForScopeEntry.getKey()).add(otherNodeValue);
            }
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

    synchronized (sharedLock) {
      List<NodeValue> nodeValueList = tokenValueMap.get(scope).get(key);
      if (nodeValueList == null) {
        nodeValueList = Collections.synchronizedList(new ArrayList<NodeValue>());
        tokenValueMap.get(scope).put(key, nodeValueList);
      }

      // Check if the current node already added the key to the token.
      // In that case replace that entry with the new one
      for (int i = 0; i < nodeValueList.size(); i++) {
        if (nodeValueList.get(i).getNodeName().equals(nodeName)) {
          nodeValueList.set(i, new NodeValue(nodeName, value));
          return;
        }
      }

      nodeValueList.add(new NodeValue(nodeName, value));
    }
  }

  @Override
  public Value get(String key) {
    return get(key, Scope.USER);
  }

  @Override
  public Value get(String key, Scope scope) {
    synchronized (sharedLock) {
      List<NodeValue> nodeValueList = tokenValueMap.get(scope).get(key);
      if (nodeValueList == null) {
        return null;
      }

      // List of NodeValue cannot be empty if the key is added in the WorkflowToken as
      // when we add key, we also add single NodeValue.
      Preconditions.checkState(!nodeValueList.isEmpty(),
                               String.format("List of NodeValue for the key %s cannot be empty", key));

      // Filter the nodeValueList so that it only contains the values added by the
      // nodes in the current node's filter list.
      Iterable<NodeValue> filteredNodeValueListIterable = Iterables.filter(nodeValueList, new Predicate<NodeValue>() {
        @Override
        public boolean apply(NodeValue input) {
          return nodeFilterSet.contains(input.getNodeName());
        }
      });

      try {
        return Iterables.getLast(filteredNodeValueListIterable).getValue();
      } catch (NoSuchElementException e) {
        return null;
      }
    }
  }

  @Override
  public Value get(String key, String nodeName) {
    return get(key, nodeName, Scope.USER);
  }

  @Override
  public Value get(String key, String nodeName, Scope scope) {
    synchronized (sharedLock) {
      List<NodeValue> nodeValueList = tokenValueMap.get(scope).get(key);
      if (nodeValueList == null) {
        return null;
      }

      for (NodeValue nodeValue : nodeValueList) {
        if (nodeValue.getNodeName().equals(nodeName)) {
          return nodeValue.getValue();
        }
      }
    }
    return null;
  }

  @Override
  public List<NodeValue> getAll(String key) {
    return getAll(key, Scope.USER);
  }

  @Override
  public List<NodeValue> getAll(String key, Scope scope) {
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
    synchronized (sharedLock) {
      ImmutableMap.Builder<String, Value> tokenValuesBuilder = ImmutableMap.builder();
      for (Map.Entry<String, List<NodeValue>> entry : tokenValueMap.get(scope).entrySet()) {
        List<NodeValue> nodeValueList = entry.getValue();
        for (NodeValue nodeValue : nodeValueList) {
          if (nodeValue.getNodeName().equals(nodeName)) {
            tokenValuesBuilder.put(entry.getKey(), nodeValue.getValue());
            break;
          }
        }
      }
      return tokenValuesBuilder.build();
    }
  }

  @Override
  public Map<String, List<NodeValue>> getAll() {
    return getAll(Scope.USER);
  }

  @Override
  public Map<String, List<NodeValue>> getAll(Scope scope) {
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
   * Make a copy of the {@link WorkflowToken}.
   * @return copied WorkflowToken
   */
  public WorkflowToken copy() {
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
