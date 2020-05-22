/*
 * Copyright © 2020 Cask Data, Inc.
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

/**
 * This class can be implemented by an {@link Aggregator} to reduce the number of group values in each split before
 * grouping all the values for the group key to achieve better performance.
 *
 * @param <GROUP_VALUE> Type of values to group
 */
@Beta
public interface Reducer<GROUP_VALUE> {

  /**
   * Reduce the given values to a single value.
   * For example, to compute the sum, the returned value will be the sum of two given values.
   * To compute average, the returned value will contain the sum and count for the two given values. Later, the
   * aggregate function can be used to compute the average.
   *
   * @param value1 the value to reduce
   * @param value2 the value to reduce
   * @return the aggregated value of two given values
   */
  GROUP_VALUE reduce(GROUP_VALUE value1, GROUP_VALUE value2) throws Exception;
}
