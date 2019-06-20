/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.api.dataset.lib.cube;

/**
 * The metrics aggregation option. It provides multiple options to aggregate the metrics:
 * TRUE: if the aggregation is set to TRUE, the metrics query will always use the total resolution table for result,
 *       which means the start and end time are ignored, and the number of data point for any metric name and tags will
 *       be 1.
 * FALSE: if the aggregation is set to FALSE, the metrics query will not do any aggregation on the data points. The
 *        resolution will be determined based on the start and end time, the number of data points returned will be
 *        based on the count specified in the query. If the number of data points > count, the latest data points will
 *        be returned. Else, all the data points will be returned. If no count is specified, all the data points will
 *        be returned.
 * SUM: if the aggregation is set to SUM, the metrics query will partition the number of metrics data points to the
 *      given count. Each partition will get aggregated to the sum of the interval. Depending on whether there is a
 *      remainder R, the first R data points will be discarded. If no count is specified or count is greater than or
 *      equal to the number of data points, all the data points will be returned.
 *      For example, if the metrics query result has 100 data points, and if the count is 10, then each 10 of the data
 *      points will be aggregated to one single point based on the aggregation option. If there are 100 points and the
 *      count is 8, since remainder is 4, first 4 data points will be discarded. The buckets will have
 *      100 / 8 = 12 data points aggregated, with 96 data points in total.
 * LATEST: if the aggregation is set to LATEST, the metrics query will behave similar to SUM. The only difference is
 *         that the data points in the partitioned interval will get aggregated to the last data point.
 */
public enum AggregationOption {
  TRUE,
  FALSE,
  SUM,
  LATEST
}
