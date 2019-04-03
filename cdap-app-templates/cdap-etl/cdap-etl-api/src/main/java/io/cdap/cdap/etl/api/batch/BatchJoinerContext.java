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

package co.cask.cdap.etl.api.batch;

import co.cask.cdap.api.annotation.Beta;

/**
 * Context of a Batch Joiner
 */
@Beta
public interface BatchJoinerContext extends BatchContext {
  /**
   * Set the number of partitions to use to join values. If none is set, the execution engine will decide
   * how many partitions to use.
   *
   * @param numPartitions the number of partitions to use when joining.
   */
  void setNumPartitions(int numPartitions);

  /**
   * Set the join key class. This is not required if the joiner is parameterized with a concrete class
   * for the join key. This method is required if the join key class is only known at configure time
   * versus compile time. For example, an joiner may support joining on a configurable record field,
   * and not know the type of that field until configure time.
   *
   * @param joinKeyClass the join key class
   */
  void setJoinKeyClass(Class<?> joinKeyClass);

  /**
   * Set the join input record class. This is not required if the joiner is parameterized with a concrete class
   * for the join input. This method is required if the input record class is only known at configure time
   * versus compile time.
   *
   * @param joinInputRecordClass the join input record class
   */
  void setJoinInputRecordClass(Class<?> joinInputRecordClass);
}
