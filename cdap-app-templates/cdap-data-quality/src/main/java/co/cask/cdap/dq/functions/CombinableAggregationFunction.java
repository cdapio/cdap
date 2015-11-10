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

package co.cask.cdap.dq.functions;

/**
 * Aggregation Function Interface
 * This is for aggregation that still make sense when combined.
 *
 * An example of such a function would be a discrete values histogram. If several
 * histograms (each corresponding to various time intervals) were combined
 * the result would be a histogram that would represent the frequencies of
 * various values over the combined time interval
 * @param <T> Aggregation type
 */
public interface CombinableAggregationFunction<T> extends BasicAggregationFunction {

  /**
   * Retrieve a combined aggregation
   */
  T retrieveAggregation();

  /**
   * Combine existing aggregations one-by-one
   */
  void combine(byte[] values);
}
