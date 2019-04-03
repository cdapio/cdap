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

package co.cask.cdap.api.metrics;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * An internal client for interacting with the metrics system.
 */
public interface MetricsSystemClient {

  /**
   * Deletes metrics based on the given {@link MetricDeleteQuery}.
   * Depending on the implementation, this method may return before metrics are actually
   * deleted.
   *
   * @param deleteQuery a {@link MetricDeleteQuery} to specific what metrics to delete
   * @throws IOException if failed to issue the delete request
   */
  void delete(MetricDeleteQuery deleteQuery) throws IOException;

  /**
   * Queries aggregated sum metrics with the max time range.
   *
   * @param tags the metrics context tags to query
   * @param metrics list of metrics name to query
   * @return a {@link Collection} of {@link MetricTimeSeries} for the query result
   */
  default Collection<MetricTimeSeries> query(Map<String, String> tags, Collection<String> metrics) throws IOException {
    return query(tags, metrics, Collections.emptySet());
  }

  /**
   * Queries aggregated sum metrics with the max time range.
   *
   * @param tags the metrics context tags to query
   * @param metrics list of metrics name to query
   * @param groupByTags the collection of tag name to group the results
   * @return a {@link Collection} of {@link MetricTimeSeries} for the query result
   */
  default Collection<MetricTimeSeries> query(Map<String, String> tags, Collection<String> metrics,
                                             Collection<String> groupByTags) throws IOException {
    return query(0, Integer.MAX_VALUE, tags, metrics, groupByTags);
  }

  /**
   * Queries aggregated sum metrics for the given time range.
   *
   * @param start minimal timestamp in seconds to query data for (inclusive)
   * @param end maximum timestamp in seconds to query data for (exclusive)
   * @param tags the metrics context tags to query
   * @param metrics list of metrics name to query
   * @return a {@link Collection} of {@link MetricTimeSeries} for the query result
   */
  default Collection<MetricTimeSeries> query(int start, int end,
                                             Map<String, String> tags, Collection<String> metrics) throws IOException {
    return query(start, end, tags, metrics, Collections.emptySet());
  }

  /**
   * Queries aggregated sun metrics for the given time range.
   *
   * @param start minimal timestamp in seconds to query data for (inclusive)
   * @param end maximum timestamp in seconds to query data for (exclusive)
   * @param tags the metrics context tags to query
   * @param metrics list of metrics name to query
   * @param groupByTags the collection of tag name to group the results
   * @return a {@link Collection} of {@link MetricTimeSeries} for the query result
   */
  default Collection<MetricTimeSeries> query(int start, int end, Map<String, String> tags,
                                             Collection<String> metrics,
                                             Collection<String> groupByTags) throws IOException {
    return query(start, end, Integer.MAX_VALUE, tags, metrics, groupByTags);
  }

  /**
   * Queries aggregated sum metrics for the given time range.
   *
   * @param start minimal timestamp in seconds to query data for (inclusive)
   * @param end maximum timestamp in seconds to query data for (exclusive)
   * @param resolution the metrics resolution in seconds
   * @param tags the metrics context tags to query
   * @param metrics list of metrics name to query
   * @param groupByTags the collection of tag name to group the results
   * @return a {@link Collection} of {@link MetricTimeSeries} for the query result
   */
  Collection<MetricTimeSeries> query(int start, int end, int resolution, Map<String, String> tags,
                                     Collection<String> metrics, Collection<String> groupByTags) throws IOException;

  /**
   * Searches for metrics names matching the given tags.
   *
   * @param tags the tags to match
   * @return the metrics matching the given tags
   * @throws IOException if failed to search
   */
  Collection<String> search(Map<String, String> tags) throws IOException;
}
