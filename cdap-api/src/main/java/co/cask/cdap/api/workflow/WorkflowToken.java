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

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Interface to represent the data that is transferred from one node to the next nodes in the {@link Workflow}.
 */
public interface WorkflowToken {

  /**
   * Put the specified key-value entry in the {@link WorkflowToken}.
   * The token may store additional information about the context in which
   * this key is being set, for example, the unique name of the workflow node.
   * @param key   the key representing the entry
   * @param value the value for the key
   */
  void put(String key, String value);

  /**
   * Same key can be added to the WorkflowToken by multiple nodes.
   * This method returns the {@link List} of {@link NodeValueEntry}, where
   * each entry represents the unique node name and the value that it set
   * for the specified key. The list maintains the order in which the values were
   * inserted in the WorkflowToken for a specific key except in the case of fork
   * and join. In case of fork in the Workflow, copies of the WorkflowToken are made
   * and passed it along the each branch. At the join, all copies of the
   * WorkflowToken are merged together. While merging, the values from the branch
   * that was completed first will be added first to the WorkflowToken.
   *
   * Example: Consider that the following values were added to the Workflow
   * for the key "myKey". Numbers associated with the values represent
   * unique node names -
   *
   *                3   4
   *            |-->D-->E--|
   * A-->B-->C-->           >-->H-->I
   * 0   1   2  |-->F-->G--|    7   8
   *                5   6
   *
   * Assume that the branch containing node 5 finishes the execution first.
   *
   * Now the method invocation getValues("myKey") will return the list
   * in which the keys will be ordered as 0-1-2-5-6-3-4-7-8.
   *
   * @param key the key to be searched
   * @return the list of {@link NodeValueEntry} from node name to the value that node
   * added for the input key
   */
  List<NodeValueEntry> getAll(String key);

  /**
   * Get the {@link Map} of key-values that were added to the {@link WorkflowToken}
   * by specific node. This method also accepts the optional prefix parameter. When
   * supplied, the map returned would be filtered by the keys prefixed by input prefix.
   * @param nodeName the unique name of the node
   * @param prefix optional prefix to filtered the keys
   * @return the map of key-values that were added by the specified node
   */
  Map<String, String> getAllFromNode(String nodeName, @Nullable String prefix);

  /**
   * Get the value set for the specified key by specified node.
   * @param key the key to be searched
   * @param nodeName the name of the node
   * @return the value set for the key by nodeName
   */
  @Nullable
  String get(String key, String nodeName);

  /**
   * Get the most recent value for the specified key.
   * @param key the key to be searched
   * @return the value for the key
   */
  @Nullable
  String get(String key);

  /**
   * This method is deprecated as of release 3.1. Instead to get the
   * MapReduce counters from the WorkflowToken, use the flatten key prefixed
   * by 'mr.counters.'.
   *
   * Example:
   * 1. To get the most recent value of counter with group name
   * 'org.apache.hadoop.mapreduce.TaskCounter' and counter name 'MAP_INPUT_RECORDS'
   *
   *  String flattenCounterKey = "mr.counters.org.apache.hadoop.mapreduce.TaskCounter.MAP_INPUT_RECORDS";
   *  workflowToken.getValue(flattenCounterKey);
   *
   * 2. To get the value of counter with group name 'org.apache.hadoop.mapreduce.TaskCounter'
   * and counter name 'MAP_INPUT_RECORDS' as set by MapReduce program with unique name 'PurchaseHistoryBuilder'
   *
   *   String flattenCounterKey = "mr.counters.org.apache.hadoop.mapreduce.TaskCounter.MAP_INPUT_RECORDS";
   *   workflowToken.getValue(flattenCounterKey, "PurchaseHistoryBuilder");
   *
   * Get the Hadoop counters from the previous MapReduce program in the Workflow.
   * The method returns null if the counters are not set.
   * @return the Hadoop MapReduce counters set by the previous MapReduce program
   */
  @Deprecated
  @Nullable
  Map<String, Map<String, Long>> getMapReduceCounters();

  /**
   * Return true if the {@link WorkflowToken} contains the specified key.
   * @param key the key to be tested for the presence in the {@link WorkflowToken}
   * @return the result of the test
   */
  boolean containsKey(String key);
}
