/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.api;

import co.cask.cdap.api.annotation.Beta;

import java.util.Iterator;

/**
 * Groups all input objects into collections and performs an aggregation on the entire group.
 * Objects that have the same group key are placed into the same group for aggregation.
 *
 * @param <GROUP_KEY> Type of group key
 * @param <GROUP_VALUE> Type of values to group
 * @param <OUT> Type of output object
 */
@Beta
public interface Aggregator<GROUP_KEY, GROUP_VALUE, OUT> {

  /**
   * Emit the group key(s) for a given input value. If no group key is emitted, the input value
   * is filtered out. If multiple group keys are emitted, the input value will be present in multiple groups.
   *
   * @param groupValue the value to group
   * @param emitter the emitter to emit zero or more group keys for the input
   * @throws Exception if there is some error getting the group
   */
  void groupBy(GROUP_VALUE groupValue, Emitter<GROUP_KEY> emitter) throws Exception;

  /**
   * Aggregate all objects in the same group into zero or more output objects.
   *
   * @param groupKey the key for the group
   * @param groupValues an iterator over all input objects that have the same group key
   * @param emitter the emitter to emit aggregate values for the group
   * @throws Exception if there is some error aggregating
   */
  void aggregate(GROUP_KEY groupKey, Iterator<GROUP_VALUE> groupValues, Emitter<OUT> emitter) throws Exception;

}
