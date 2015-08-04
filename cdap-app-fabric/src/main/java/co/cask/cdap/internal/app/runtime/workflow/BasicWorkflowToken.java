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
import co.cask.cdap.common.conf.Constants;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the {@link WorkflowToken} interface.
 */
public class BasicWorkflowToken implements WorkflowToken, Serializable {

  private static final long serialVersionUID = -1173500180640174909L;

  private final Map<Scope, Map<String, List<NodeValue>>> tokenValueMap = new EnumMap<>(Scope.class);
  private final int maxSizeBytes;
  private Map<String, Map<String, Long>> mapReduceCounters;
  private String nodeName;
  private boolean putAllowed = true;
  private int bytesLeft;

  /**
   * Creates a {@link BasicWorkflowToken} with the specified maximum size.
   *
   * @param maxSizeMb the specified maximum size in MB for the {@link BasicWorkflowToken} to create.
   */
  public BasicWorkflowToken(int maxSizeMb) {
    for (Scope scope : Scope.values()) {
      this.tokenValueMap.put(scope, new HashMap<String, List<NodeValue>>());
    }
    this.maxSizeBytes = maxSizeMb * 1024 * 1024;
    this.bytesLeft = maxSizeBytes;
  }

  private BasicWorkflowToken(BasicWorkflowToken other) {
    for (Map.Entry<Scope, Map<String, List<NodeValue>>> entry : other.tokenValueMap.entrySet()) {
      Map<String, List<NodeValue>> tokenValueMapForScope = new HashMap<>();
      for (Map.Entry<String, List<NodeValue>> valueEntry : entry.getValue().entrySet()) {
        tokenValueMapForScope.put(valueEntry.getKey(), Lists.newArrayList(valueEntry.getValue()));
      }

      this.tokenValueMap.put(entry.getKey(), tokenValueMapForScope);
    }

    this.nodeName = other.nodeName;

    if (other.mapReduceCounters != null) {
      this.mapReduceCounters = copyHadoopCounters(other.mapReduceCounters);
    }
    this.maxSizeBytes = other.maxSizeBytes;
    this.bytesLeft = other.bytesLeft;
  }

  void setCurrentNode(String nodeName) {
    this.nodeName = nodeName;
  }

  /**
   * Method to disable the put operation on the {@link WorkflowToken} form Mapper and Reducer classes.
   */
  public void disablePut() {
    putAllowed = false;
  }

  /**
   * Merge the other WorkflowToken passed to the method as a parameter
   * with the WorkflowToken on which the method is invoked.
   * @param other the other WorkflowToken to be merged
   */
  void mergeToken(BasicWorkflowToken other) {
    for (Map.Entry<Scope, Map<String, List<NodeValue>>> entry : other.tokenValueMap.entrySet()) {
      Map<String, List<NodeValue>> thisTokenValueMapForScope = this.tokenValueMap.get(entry.getKey());

      for (Map.Entry<String, List<NodeValue>> otherTokenValueMapForScopeEntry : entry.getValue().entrySet()) {
        String otherKey = otherTokenValueMapForScopeEntry.getKey();
        if (!thisTokenValueMapForScope.containsKey(otherKey)) {
          thisTokenValueMapForScope.put(otherKey, Lists.<NodeValue>newArrayList());
        }

        // Iterate over the list of NodeValue corresponding to the current key.
        // Only add those NodeValue to the merged token which already do not exist.

        for (NodeValue otherNodeValue : otherTokenValueMapForScopeEntry.getValue()) {
          boolean otherNodeValueExist = false;
          for (NodeValue thisNodeValue : thisTokenValueMapForScope.get(otherKey)) {
            if (thisNodeValue.equals(otherNodeValue)) {
              otherNodeValueExist = true;
              break;
            }
          }
          if (!otherNodeValueExist) {
            addOrUpdate(otherKey, otherNodeValue, thisTokenValueMapForScope.get(otherKey), -1);
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
    if (!putAllowed) {
      String msg = String.format("Failed to put key '%s' from node '%s' in the WorkflowToken. Put operation is not " +
                                 "allowed from the Mapper and Reducer classes and from Spark executor.", key, nodeName);
      throw new UnsupportedOperationException(msg);
    }

    Preconditions.checkNotNull(key, "Null key cannot be added in the WorkflowToken.");
    Preconditions.checkNotNull(value, String.format("Null value provided for the key '%s'.", key));
    Preconditions.checkNotNull(value.toString(), String.format("Null value provided for the key '%s'.", key));
    Preconditions.checkState(nodeName != null, "nodeName cannot be null.");

    List<NodeValue> nodeValueList = tokenValueMap.get(scope).get(key);
    if (nodeValueList == null) {
      nodeValueList = Lists.newArrayList();
      tokenValueMap.get(scope).put(key, nodeValueList);
    }

    NodeValue nodeValueToAddUpdate = new NodeValue(nodeName, value);
    // Check if the current node already added the key to the token.
    // In that case replace that entry with the new one
    for (int i = 0; i < nodeValueList.size(); i++) {
      NodeValue existingNodeValue = nodeValueList.get(i);
      if (existingNodeValue.getNodeName().equals(nodeName)) {
        addOrUpdate(key, nodeValueToAddUpdate, nodeValueList, i);
        return;
      }
    }

    addOrUpdate(key, nodeValueToAddUpdate, nodeValueList, -1);
  }

  @Override
  public Value get(String key) {
    return get(key, Scope.USER);
  }

  @Override
  public Value get(String key, Scope scope) {
    List<NodeValue> nodeValueList = tokenValueMap.get(scope).get(key);
    if (nodeValueList == null) {
      return null;
    }

    // List of NodeValue cannot be empty if the key is added in the WorkflowToken as
    // when we add key, we also add single NodeValue.
    Preconditions.checkState(!nodeValueList.isEmpty(),
                             String.format("List of NodeValue for the key %s cannot be empty", key));

    return nodeValueList.get(nodeValueList.size() - 1).getValue();
  }

  @Override
  public Value get(String key, String nodeName) {
    return get(key, nodeName, Scope.USER);
  }

  @Override
  public Value get(String key, String nodeName, Scope scope) {
    List<NodeValue> nodeValueList = tokenValueMap.get(scope).get(key);
    if (nodeValueList == null) {
      return null;
    }

    for (NodeValue nodeValue : nodeValueList) {
      if (nodeValue.getNodeName().equals(nodeName)) {
        return nodeValue.getValue();
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
  
  /**
   * Updates a key in the workflow token. Used to either add or update the {@link NodeValue} for a key, depending on
   * whether it exists already.
   *
   * @param key the key whose value is to be added or updated.
   * @param nodeValue the {@link NodeValue} to add or update
   * @param nodeValues the existing, non-null list of {@link NodeValue} for the specified key
   * @param index the index at which to add or update. For adding, use a number less than 0, for replacing,
   */
  private void addOrUpdate(String key, NodeValue nodeValue, List<NodeValue> nodeValues, int index) {
    int oldValueLen = (index < 0) ? 0 : nodeValues.get(index).getValue().toString().length();
    int valueLen = nodeValue.getValue().toString().length();

    int left = bytesLeft - valueLen + oldValueLen;
    left = (left < 0 || index >= 0) ? left : left - key.length();
    if (left < 0) {
      throw new IllegalStateException(String.format("Exceeded maximum permitted size of workflow token '%sMB' while " +
                                                      "adding key '%s' with value '%s'. Current size is '%sMB'. " +
                                                      "Please increase the maximum permitted size by setting the " +
                                                      "parameter '%s' in cdap-site.xml to add more values.",
                                                    maxSizeBytes / (1024 * 1024), key, nodeValue,
                                                    (maxSizeBytes - bytesLeft) / (1024 * 1024),
                                                    Constants.AppFabric.WORKFLOW_TOKEN_MAX_SIZE_MB));
    }
    if (index >= 0) {
      nodeValues.set(index, nodeValue);
    } else {
      nodeValues.add(nodeValue);
    }
    bytesLeft = left;
  }

  // Serialize the WorkflowToken content for passing it to the Spark executor.
  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
  }

  // Deserialize the WorkflowToken for using it inside the Spark executor. Set the putAllowed
  // flag to false so that we do not allow putting the values inside the Spark executor.
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    putAllowed = false;
  }
}
