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

package co.cask.cdap.metrics.store;

import co.cask.cdap.metrics.store.cube.CubeExploreQuery;
import co.cask.cdap.metrics.store.cube.CubeQuery;
import co.cask.cdap.metrics.store.cube.TimeSeries;
import co.cask.cdap.metrics.store.timeseries.TagValue;
import co.cask.cdap.metrics.transport.MetricValue;

import java.util.Collection;

/**
 * Stores metric system data.
 */
// todo: methods should throw IOException instead of Exception
public interface MetricStore {
  /**
   * Adds {@link MetricValue} to the store.
   * @param metricValue metric value to add.
   * @throws Exception
   */
  void add(MetricValue metricValue) throws Exception;

  /**
   * Queries metrics data.
   * @param query query to execute
   * @return time series that satisfy the query
   * @throws Exception
   */
  // todo: expose metric-specific APIs instead of Cube ones
  Collection<TimeSeries> query(CubeQuery query) throws Exception;

  /**
   * Deletes all metric data before given timestamp. Used for applying TTL policy.
   * @param timestamp time up to which to delete metrics data, in ms since epoch
   */
  void deleteBefore(long timestamp);

  /**
   * todo
   * @param query
   * @return
   */
  Collection<TagValue> getNextTags(CubeExploreQuery query) throws Exception;

  /**
   * todo
   * @param query
   * @return
   */
  Collection<String> getMeasureNames(CubeExploreQuery query) throws Exception;

}
