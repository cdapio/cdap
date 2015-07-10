/*
 * Copyright 2015 Cask Data, Inc.
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

package co.cask.cdap.api.metrics;

import java.util.Collection;

/**
 * Stores and provides access to metrics data.
 */
// todo: methods should throw IOException instead of Exception
public interface MetricStore {
  /**
   * Sets {@link MetricsContext} to be used for emitting metrics by this {@link MetricStore}.
   * @param metricsContext metrics context to use
   */
  void setMetricsContext(MetricsContext metricsContext);

  /**
   * Adds {@link MetricValues} to the store.
   * @param metricValues metric values to add.
   * @throws Exception
   */
  void add(MetricValues metricValues) throws Exception;

  /**
   * Adds {@link MetricValues}s to the store.
   * @param metricValues metric values to add.
   * @throws Exception
   */
  void add(Collection<? extends MetricValues> metricValues) throws Exception;

  /**
   * Queries metrics data.
   * @param query query to execute
   * @return time series that satisfy the query
   * @throws Exception
   */
  Collection<MetricTimeSeries> query(MetricDataQuery query) throws Exception;

  /**
   * Deletes all metric data before given timestamp. Used for applying TTL policy.
   * @param timestamp time up to which to delete metrics data, in ms since epoch
   */
  void deleteBefore(long timestamp) throws Exception;

  /**
   * Deletes all metric data specified by the {@link MetricDeleteQuery}
   * @param query specifies what to delete
   */
  void delete(MetricDeleteQuery query) throws Exception;

  /**
   * Deletes all metrics data. NOTE: dangerous, all data will be lost. Likely you only need to use it in tests.
   * @throws Exception
   */
  void deleteAll() throws Exception;

  /**
   * Given a list of tags in the {@link MetricSearchQuery}, returns the list of next available tags
   * @param query specifies where to search
   * @return collection of tag value pairs in no particular order
   */
  Collection<TagValue> findNextAvailableTags(MetricSearchQuery query) throws Exception;

  /**
   * Given a list of tags in the {@link MetricSearchQuery}, returns the list of measures available
   * @param query specifies where to search
   * @return collection of metric names in no particular order
   */
  Collection<String> findMetricNames(MetricSearchQuery query) throws Exception;
}
