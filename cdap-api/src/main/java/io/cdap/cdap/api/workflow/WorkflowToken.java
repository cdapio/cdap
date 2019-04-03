/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.Beta;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Interface to represent the data that is transferred from one node to the next nodes in the {@link Workflow}.
 */
@Beta
public interface WorkflowToken {

  /**
   * Keys in the {@link WorkflowToken} can be added by user, using the
   * {@link WorkflowToken#put} method. These keys are added under the {@link Scope#USER} scope.
   * CDAP also adds some keys to the {@link WorkflowToken}. for e.g. MapReduce counters.
   * The keys added by CDAP are added under the {@link Scope#SYSTEM} scope.
   */
  enum Scope {
    USER,
    SYSTEM
  }

  /**
   * Put the specified key and value into the {@link WorkflowToken}.
   * The token may store additional information about the context in which
   * this key is being set. For example, the context information stored by the
   * token may be the workflow node that is performing the operation, or the name
   * of the workflow if the operation is performed in {@link AbstractWorkflow#initialize}
   * or {@link AbstractWorkflow#destroy} method.
   * @param key the key representing the entry
   * @param value the value for the key
   * @throws UnsupportedOperationException if called in a context where the token may not be modified.
   */
  void put(String key, String value);

  /**
   * Put the specified key and {@link Value} into the {@link WorkflowToken}.
   * The token may store additional information about the context in which
   * this key is being set. For example, the context information stored by the
   * token may be the workflow node that is performing the operation, or the name
   * of the workflow if the operation is performed in {@link AbstractWorkflow#initialize}
   * or {@link AbstractWorkflow#destroy} method.
   * @param key the key representing entry
   * @param value the {@link Value} for the key
   * @throws UnsupportedOperationException if called in a context where the token may not be modified.
   */
  void put(String key, Value value);

  /**
   * Get the most recent value added for the specified key for a {@link Scope#USER} scope.
   * @param key the key to be searched
   * @return the {@link Value} for the key or <code>null</code> if the key does not
   * exist in the {@link Scope#USER} scope
   */
  @Nullable
  Value get(String key);

  /**
   * Get the most recent value for the specified key for a given scope.
   * @param key the key to be searched
   * @param scope the {@link WorkflowToken.Scope} for the key
   * @return the {@link Value} for the key from the specified scope or <code>null</code> if the key
   * does not exist in the given scope
   */
  @Nullable
  Value get(String key, Scope scope);

  /**
   * Get the value set for the specified key by the specified node for a {@link Scope#USER} scope.
   * To get the token values set from {@link AbstractWorkflow#initialize} or
   * {@link AbstractWorkflow#destroy} methods, provide the name of the Workflow as nodeName.
   * @param key the key to be searched
   * @param nodeName the name of the node
   * @return the {@link Value} set for the key by nodeName or <code>null</code> if the key is not
   * added by the nodeName in the {@link Scope#USER} scope
   */
  @Nullable
  Value get(String key, String nodeName);

  /**
   * Get the value set for the specified key by the specified node for a given scope.
   * To get the token values set from {@link AbstractWorkflow#initialize} or
   * {@link AbstractWorkflow#destroy} methods, provide the name of the Workflow as nodeName.
   * @param key the key to be searched
   * @param nodeName the name of the node
   * @param scope the {@link WorkflowToken.Scope} for the key
   * @return the {@link Value} set for the key by nodeName for a given scope or <code>null</code>
   * if the key is not added by the nodeName in the given scope
   */
  @Nullable
  Value get(String key, String nodeName, Scope scope);

  /**
   * Same key can be added to the {@link WorkflowToken} by multiple nodes.
   * This method returns the {@link List} of {@link NodeValue}, where
   * each entry represents the unique node name and the {@link Value} that it set
   * for the specified key for a {@link Scope#USER} scope.
   * <p>
   * The list maintains the order in which the values were
   * inserted in the WorkflowToken for a specific key except in the case of fork
   * and join. In case of fork in the Workflow, copies of the WorkflowToken are made
   * and passed along each branch. At the join, all copies of the
   * WorkflowToken are merged together. While merging, the order in which the values were
   * inserted for a specific key is guaranteed within the same branch, but not across
   * different branches.
   * @param key the key to be searched
   * @return the list of {@link NodeValue} from node name to the value that node
   * added for the input key
   */
  List<NodeValue> getAll(String key);

  /**
   * Same key can be added to the WorkflowToken by multiple nodes.
   * This method returns the {@link List} of {@link NodeValue}, where
   * each entry represents the unique node name and the {@link Value} that it set
   * for the specified key for a given scope.
   * <p>
   * The list maintains the order in which the values were
   * inserted in the WorkflowToken for a specific key except in the case of fork
   * and join. In case of fork in the Workflow, copies of the WorkflowToken are made
   * and passed along each branch. At the join, all copies of the
   * WorkflowToken are merged together. While merging, the order in which the values were
   * inserted for a specific key is guaranteed within the same branch, but not across
   * different branches.
   * @param key the key to be searched
   * @param scope the {@link WorkflowToken.Scope} for the key
   * @return the list of {@link NodeValue} from node name to the value that node
   * added for the input key for a given scope
   */
  List<NodeValue> getAll(String key, Scope scope);

  /**
   * Get the {@link Map} of key to {@link Value}s that were added to the {@link WorkflowToken}
   * by specific node for a {@link Scope#USER} scope. To get the token values set from
   * {@link AbstractWorkflow#initialize} or {@link AbstractWorkflow#destroy} methods, provide
   * the name of the Workflow as nodeName.
   * @param nodeName the unique name of the node
   * @return the map of key to values that were added by the specified node
   */
  Map<String, Value> getAllFromNode(String nodeName);

  /**
   * Get the {@link Map} of key to {@link Value}s that were added to the {@link WorkflowToken}
   * by specific node for a given scope. To get the token values set from
   * {@link AbstractWorkflow#initialize} or {@link AbstractWorkflow#destroy} methods, provide
   * the name of the Workflow as nodeName.
   * @param nodeName the unique name of the node
   * @param scope the {@link WorkflowToken.Scope} for the key
   * @return the map of key to values that were added by the specified node for a given scope
   */
  Map<String, Value> getAllFromNode(String nodeName, Scope scope);

  /**
   * Same key can be added to the WorkflowToken by multiple nodes.
   * This method returns the key to {@link List} of {@link NodeValue}
   * added in the {@link Scope#USER} scope.
   * @return the {@link Map} of key to {@link List} of {@link NodeValue} added for
   * the given scope
   */
  Map<String, List<NodeValue>> getAll();

  /**
   * Same key can be added to the WorkflowToken by multiple nodes.
   * This method returns the key to {@link List} of {@link NodeValue}
   * added in the {@link WorkflowToken.Scope} provided.
   * @param scope the scope for the key
   * @return the {@link Map} of key to {@link List} of {@link NodeValue} added for
   * the given scope
   */
  Map<String, List<NodeValue>> getAll(Scope scope);
}
