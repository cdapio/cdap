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

package co.cask.cdap.api.workflow;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Interface to represent the data that is transferred from one node to the next node in the {@link Workflow}.
 */
public interface WorkflowToken {

  /**
   * Put the specified key-value entry in the {@link WorkflowToken}.
   * The token may store additional information about the context in which
   * this key is being set, for example, the unique name of the workflow node.
   * @param key   the key representing the entry
   * @param value the value for the key
   */
  void setValue(String key, String value);

  /**
   * Get the name of the node which most recently set the value for the key.
   * Multiple nodes in the Workflow can add the same key. In this case, the name of the node that most
   * recently added the key to the {@link WorkflowToken} will be returned.
   * @param key the key to be searched
   * @return the name of the node that most recently set the value for the key
   */
  @Nullable
  String getLastSetter(String key);

  /**
   * Get the most recent value for the key. Return value does not
   * necessarily indicate that the {@link WorkflowToken} contains no
   * value for the key; it's also possible that the value of the key
   * was explicitly set to {@code null}. The {@link #containsKey containsKey}
   * operation may be used to distinguish these two cases.
   * Multiple nodes in the Workflow can add the same key. In this case,
   * the value that was most recently added for the key in the {@link WorkflowToken}
   * will be returned.
   * @param key the key to be searched
   * @return the most recent value that was set for the key.
   */
  @Nullable
  String getValue(String key);

  /**
   * Get the value for the key set by particular node.
   * @param key the key to be searched for
   * @param nodeName the unique name of the node
   * @return the value set for the key by nodeName
   */
  @Nullable
  String getValue(String key, String nodeName);

  /**
   * Same key can be added to the WorkflowToken by multiple nodes.
   * This method returns the map of node name to the value which was set by the node for this key.
   * @param key the key to be searched
   * @return the map of node name to the value
   */
  Map<String, String> getAllValues(String key);

  /**
   * Get the Hadoop counters from the previous MapReduce program in the Workflow. The method returns null
   * if the counters are not set.
   * @return the Hadoop MapReduce counters set by the previous MapReduce program
   */
  @Nullable
  Map<String, Map<String, Long>> getMapReduceCounters();

  /**
   * Return true if the {@link WorkflowToken} contains the specified key.
   * @param key the key to be tested for the presence in the {@link WorkflowToken}
   * @return the result of the test
   */
  boolean containsKey(String key);
}
