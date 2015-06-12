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
   * Put the specified key-value entry in the WorkflowToken.
   * The entry is stored against the node at which it is being added.
   * @param key   the key representing the entry
   * @param value the value for the key
   */
  void putValue(String key, String value);

  /**
   * Get the latest value for the key from the WorkflowToken.
   * Multiple nodes in the Workflow can add same key. In this case, the value that was most recently added
   * will be returned.
   * @param key the key to be searched
   * @return the most recent value that was set for the key
   */
  @Nullable
  String getValue(String key);

  /**
   * Same key can be added to the WorkflowToken by multiple nodes.
   * This method returns the map of node name to the value which was set by the node for this key.
   * @param key the key to be searched
   * @return the map of node name to the value
   */
  @Nullable
  Map<String, TokenValueWithTimestamp> getAllValues(String key);

  /**
   * Get the Hadoop counters from the previous MapReduce program in the Workflow. The method returns null
   * if the counters are not set.
   * @return the Hadoop MapReduce counters set by the previous MapReduce program
   */
  @Nullable
  Map<String, Map<String, Long>> getMapReduceCounters();
}
