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

package io.cdap.cdap.etl.api;

import io.cdap.cdap.api.annotation.Beta;

import java.util.Collection;
import java.util.Collections;

/**
 * Provides join keys on which join needs to be performed and merges the join results.
 *
 * @param <JOIN_KEY>     type of the join key
 * @param <INPUT_RECORD> type of input records to be joined
 * @param <OUT>          type of output object
 */
@Beta
public interface Joiner<JOIN_KEY, INPUT_RECORD, OUT> {

  /**
   * Return value for the join key on which join will be performed
   *
   * @param stageName   name of the stage to which records belongs to
   * @param inputRecord input record to be joined
   * @return returns join key
   * @throws Exception if there is some error getting the join key
   * @deprecated {@link #getJoinKeys(String, Object)} should be used instead
   */
  @Deprecated
  default JOIN_KEY joinOn(String stageName, INPUT_RECORD inputRecord) throws Exception {
    throw new UnsupportedOperationException();
  };

  /**
   * Return value for the join keys on which join will be performed
   *
   * @param stageName   name of the stage to which records belongs to
   * @param inputRecord input record to be joined
   * @return returns a list of join keys
   * @throws Exception if there is some error getting the join key
   */
  default Collection<JOIN_KEY> getJoinKeys(String stageName, INPUT_RECORD inputRecord) throws Exception {
    return Collections.singletonList(joinOn(stageName, inputRecord));
  }

  ;

  /**
   * Creates join configuration which holds information about required inputs which are needed to decide
   * type of the join and produce join result.
   *
   * @return instance of {@link JoinConfig} which includes information about join to be performed
   * @throws Exception if there is some error getting the join config
   */
  JoinConfig getJoinConfig() throws Exception;

  /**
   * Merges records present in joinResult and returns merged output.
   *
   * @param joinKey    join key on which join needs to be performed
   * @param joinResult list of {@link JoinElement} which will be used to create merged output. It will have all the
   *                   records after performing join operation
   * @return merged output created from joinResult
   * @throws Exception if there is some error while creating merged output
   */
  OUT merge(JOIN_KEY joinKey, Iterable<JoinElement<INPUT_RECORD>> joinResult) throws Exception;
}
