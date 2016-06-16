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

import java.util.List;

/**
 * Provides join keys on which join needs to be performed and merges the join results.
 *
 * @param <JOIN_KEY> Type of the join key
 * @param <JOIN_VALUE> Type of the values to be joined
 * @param <OUT> Type of output object
 */
@Beta
public interface Joiner<JOIN_KEY, JOIN_VALUE, OUT> {

  /**
   * Return values for join key on which join will be performed
   *
   * @param stageName name of the stage to which records belongs to
   * @param joinValue value to be joined
   * @return returns join key
   * @throws Exception if there is some error getting the join key
   */
  JOIN_KEY joinOn(String stageName, JOIN_VALUE joinValue) throws Exception;


  /**
   * Creates join configuration which is needed for performing join
   *
   * @return JoinConfiguration which includes information about join to be performed.
   * @throws Exception if there is some error getting the join config
   */
  JoinConfig getJoinConfig() throws Exception;

  /**
   * Merge together each {@link JoinElement} in {@link JoinResult} based on output Schema to generate output
   *
   * @param joinKey Join key on which join is performed
   * @param joinResults List of join results to produce output
   * @param emitter emitter to emit output
   * @throws Exception if there is some error emitting the output
   */
  void merge(JOIN_KEY joinKey, List<JoinResult> joinResults, Emitter<OUT> emitter) throws Exception;
}
